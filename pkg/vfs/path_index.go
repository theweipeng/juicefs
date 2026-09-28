package vfs

import (
	"container/list"
	"strings"
	"time"
)

const (
	// DefaultChangeIndexLimit 限制当前与历史关系总数，避免故障时持续增长。
	DefaultChangeIndexLimit = 262144
	changeIndexIdle         = 5 * time.Minute
	changeIndexBatch        = 128
	changePathLimit         = 4096
	changePathWorkLimit     = 16384
)

type pathEdge struct {
	parent Ino
	name   string
}

type pathRecord struct {
	edge       pathEdge
	inode      Ino
	generation uint64
	seen       time.Time
	retired    bool
	element    *list.Element
}

type pathReceipt struct {
	record     *pathRecord
	generation uint64
}

// 当前关系与待失效的旧关系分别排队；只在失效成功后按代次回收。
type pathIndex struct {
	edges      map[pathEdge]map[Ino]*pathRecord
	parents    map[Ino]map[pathEdge]*pathRecord
	current    map[pathEdge]*pathRecord
	links      map[Ino]uint32
	children   map[Ino]int
	active     list.List
	retired    list.List
	generation uint64
	limit      int
	incomplete bool
	overflows  uint64
	reclaimed  uint64
}

func newPathIndex() *pathIndex {
	return &pathIndex{
		edges:    make(map[pathEdge]map[Ino]*pathRecord),
		parents:  make(map[Ino]map[pathEdge]*pathRecord),
		current:  make(map[pathEdge]*pathRecord),
		links:    make(map[Ino]uint32),
		children: make(map[Ino]int),
		limit:    DefaultChangeIndexLimit,
	}
}

func (p *pathIndex) size() int { return p.active.Len() + p.retired.Len() }

func (p *pathIndex) underPressure() bool { return p.size() >= p.limit-p.limit/4 }

func (p *pathIndex) remember(parent Ino, name string, inode Ino, links ...uint32) {
	if name == "." || name == ".." || strings.Contains(name, "/") || name == "" {
		return
	}
	edge := pathEdge{parent, name}
	p.generation++
	if previous := p.current[edge]; previous != nil && previous.inode != inode {
		p.active.Remove(previous.element)
		previous.retired = true
		previous.generation = p.generation
		previous.element = p.retired.PushBack(previous)
		delete(p.current, edge)
	}
	record := p.edges[edge][inode]
	if record == nil {
		if p.size() >= p.limit {
			// 未记录的内核缓存无法证明已清理；不阻塞文件操作，也不能返回成功失效。
			p.incomplete = true
			p.overflows++
			return
		}
		record = &pathRecord{edge: edge, inode: inode}
		if p.edges[edge] == nil {
			p.edges[edge] = make(map[Ino]*pathRecord)
		}
		p.edges[edge][inode] = record
		p.children[parent]++
		if inode != 0 {
			if p.parents[inode] == nil {
				p.parents[inode] = make(map[pathEdge]*pathRecord)
			}
			p.parents[inode][edge] = record
			if _, known := p.links[inode]; !known {
				p.links[inode] = 1
			}
		}
	} else if record.retired {
		p.retired.Remove(record.element)
	} else {
		p.active.Remove(record.element)
	}
	record.retired = false
	record.generation = p.generation
	record.seen = time.Now()
	record.element = p.active.PushBack(record)
	p.current[edge] = record
	if inode != 0 && len(links) > 0 {
		p.links[inode] = links[0]
	}
}

func (p *pathIndex) remove(receipt pathReceipt) bool {
	r := receipt.record
	if p.edges[r.edge][r.inode] != r || r.generation != receipt.generation {
		return false
	}
	if !r.retired && r.inode != 0 && p.children[r.inode] > 0 {
		return false
	}
	if r.retired {
		p.retired.Remove(r.element)
	} else {
		p.active.Remove(r.element)
	}
	delete(p.edges[r.edge], r.inode)
	p.children[r.edge.parent]--
	if p.children[r.edge.parent] == 0 {
		delete(p.children, r.edge.parent)
	}
	if len(p.edges[r.edge]) == 0 {
		delete(p.edges, r.edge)
	}
	if p.current[r.edge] == r {
		delete(p.current, r.edge)
	}
	if r.inode != 0 {
		delete(p.parents[r.inode], r.edge)
		if len(p.parents[r.inode]) == 0 {
			delete(p.parents, r.inode)
			delete(p.links, r.inode)
		}
	}
	p.reclaimed++
	return true
}

func (p *pathIndex) retry(receipt pathReceipt) {
	r := receipt.record
	if p.edges[r.edge][r.inode] != r || r.generation != receipt.generation {
		return
	}
	if r.retired {
		p.retired.MoveToBack(r.element)
	} else {
		r.seen = time.Now()
		p.active.MoveToBack(r.element)
	}
}

func (p *pathIndex) candidates(now time.Time) []pathReceipt {
	result := make([]pathReceipt, 0, changeIndexBatch)
	for e := p.retired.Front(); e != nil && len(result) < changeIndexBatch; e = e.Next() {
		r := e.Value.(*pathRecord)
		result = append(result, pathReceipt{r, r.generation})
	}
	pressure := p.underPressure()
	for e := p.active.Front(); e != nil && len(result) < changeIndexBatch; e = e.Next() {
		r := e.Value.(*pathRecord)
		if !pressure && now.Sub(r.seen) < changeIndexIdle {
			break
		}
		if r.inode != 0 && p.children[r.inode] > 0 {
			continue
		}
		result = append(result, pathReceipt{r, r.generation})
	}
	return result
}

// paths 同时返回完整性；丢失父目录、环、深度或展开预算耗尽都不能当作完整结果。
func (p *pathIndex) paths(inode Ino, historical bool) ([]string, bool) {
	complete := !p.incomplete
	var result []string
	seen := make(map[Ino]bool)
	names := make([]string, 0, 16)
	work, bytes := 0, 0
	var walk func(Ino)
	walk = func(ino Ino) {
		work++
		if work > changePathWorkLimit || len(names) > 256 {
			complete = false
			return
		}
		if ino == rootID {
			if len(result) >= changePathLimit {
				complete = false
				return
			}
			parts := make([]string, len(names))
			for i, name := range names {
				parts[len(names)-1-i] = name
			}
			value := "/" + strings.Join(parts, "/")
			bytes += len(value)
			if bytes > 1024*1024 {
				complete = false
				return
			}
			result = append(result, value)
			return
		}
		if seen[ino] {
			complete = false
			return
		}
		seen[ino] = true
		defer delete(seen, ino)
		current := 0
		for _, record := range p.parents[ino] {
			if !record.retired {
				current++
			}
		}
		if p.links[ino] == 0 || uint32(current) < p.links[ino] {
			complete = false
		}
		found := false
		for edge, record := range p.parents[ino] {
			if record.retired && !historical {
				continue
			}
			found = true
			names = append(names, edge.name)
			walk(edge.parent)
			names = names[:len(names)-1]
			if work > changePathWorkLimit || len(result) >= changePathLimit || bytes > 1024*1024 {
				complete = false
				break
			}
		}
		if !found {
			complete = false
		}
	}
	walk(inode)
	return result, complete && len(result) > 0
}

func covers(root, candidate string, recursive bool) bool {
	return root == candidate || recursive && strings.HasPrefix(candidate, strings.TrimSuffix(root, "/")+"/")
}
