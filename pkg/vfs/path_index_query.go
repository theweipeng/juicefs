package vfs

import (
	"path"
	"strings"
	"syscall"
)

// checkPositions 只校验发生变化的父链；未定位子树不能借机扩大清除。
func (p *pathIndex) checkPositions(changes []FileChange, scope *changeMatcher, trace *invalidationTrace) error {
	if len(p.positionChecks) == 0 {
		return nil
	}
	pending := make([]Ino, 0, len(p.positionChecks))
	for parent := range p.positionChecks {
		pending = append(pending, parent)
	}
	visited := make(map[Ino]bool)
	checked := 0
	for len(pending) > 0 {
		parent := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if visited[parent] {
			continue
		}
		visited[parent] = true
		entries := p.entries[parent]
		if len(entries) == 0 {
			delete(p.positionChecks, parent)
			continue
		}
		checked++
		if checked > p.limit {
			trace.phase("index_scope", parent, pathEdge{})
			trace.detail(map[string]interface{}{"scope_reason": "position_work_budget", "position_checked_parents": checked})
			return syscall.EAGAIN
		}
		parents, complete := p.paths(parent, true)
		if complete {
			delete(p.positionChecks, parent)
			continue
		}
		for name, targets := range entries {
			proven := false
			for _, prefix := range parents {
				value := path.Join(prefix, name)
				for _, change := range changes {
					proven = proven || covers(change.Path, value, change.Recursive) && (scope == nil || scope.matches(value, true))
				}
			}
			if !proven {
				trace.phase("index_scope", 0, pathEdge{parent, name})
				selected := make([]string, 0, 20)
				for _, prefix := range parents {
					if len(selected) == 20 {
						break
					}
					selected = append(selected, diagnosticText(prefix, 2048))
				}
				trace.detail(map[string]interface{}{"scope_reason": "incomplete_parent_paths", "unresolved_parent_inode": parent, "known_parent_paths": selected, "position_checked_parents": checked})
				return syscall.EAGAIN
			}
			// 只有存在缺口的子树需要继续核验，已定位的无关目录不参加检查。
			for inode := range targets {
				if inode != 0 && len(p.entries[inode]) > 0 {
					pending = append(pending, inode)
				}
			}
		}
	}
	trace.detail(map[string]interface{}{"position_checked_parents": checked})
	return nil
}

// matchPaths 沿目录项定位目标，只展开命中的子树及历史关系。
func (p *pathIndex) matchPaths(changes []FileChange, scope *changeMatcher, fullRoot bool, trace *invalidationTrace) (map[Ino]bool, map[pathEdge]bool, []pathReceipt, error) {
	inodes, edges := make(map[Ino]bool), make(map[pathEdge]bool)
	var retired []pathReceipt
	retiredEdges := make(map[pathEdge]bool)
	add := func(edge pathEdge, retire bool) {
		edges[edge], inodes[edge.parent] = true, true
		for inode, record := range p.edges[edge] {
			if inode != 0 {
				inodes[inode] = true
			}
			if retire && record.retired && !retiredEdges[edge] {
				retired = append(retired, pathReceipt{record, record.generation})
			}
		}
		retiredEdges[edge] = retiredEdges[edge] || retire
	}
	if fullRoot {
		for edge := range p.edges {
			add(edge, true)
		}
		trace.detail(map[string]interface{}{"lookup_mode": "explicit_volume", "scanned_edges": len(p.edges)})
		return inodes, edges, retired, nil
	}
	if err := p.checkPositions(changes, scope, trace); err != nil {
		return nil, nil, nil, err
	}
	type location struct {
		inode Ino
		value string
	}
	visited := make(map[location]bool)
	work := 0
	step := func() error {
		work++
		if work > p.limit*2+len(changes)*256 {
			trace.phase("index_scope", 0, pathEdge{})
			trace.detail(map[string]interface{}{"scope_reason": "query_work_budget", "scanned_edges": work})
			return syscall.EAGAIN
		}
		return nil
	}
	var descend func(Ino, string, int) error
	descend = func(inode Ino, prefix string, depth int) error {
		key := location{inode, prefix}
		if visited[key] {
			return nil
		}
		visited[key] = true
		if depth > 256 {
			trace.phase("index_scope", inode, pathEdge{})
			return syscall.EAGAIN
		}
		for name, targets := range p.entries[inode] {
			if err := step(); err != nil {
				return err
			}
			value := path.Join(prefix, name)
			if scope != nil && !scope.matches(value, true) {
				continue
			}
			add(pathEdge{inode, name}, scope == nil || scope.matches(value, false))
			for target := range targets {
				if target != 0 {
					if err := descend(target, value, depth+1); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, change := range changes {
		if change.Path == "/" {
			inodes[rootID] = true
			if change.Recursive {
				if err := descend(rootID, "/", 0); err != nil {
					return nil, nil, nil, err
				}
			}
			continue
		}
		parts := strings.Split(strings.TrimPrefix(change.Path, "/"), "/")
		if len(parts) > 256 {
			trace.phase("index_scope", 0, pathEdge{})
			return nil, nil, nil, syscall.EAGAIN
		}
		parents := map[Ino]bool{rootID: true}
		prefix := ""
		for i, name := range parts {
			prefix += "/" + name
			next := make(map[Ino]bool)
			for parent := range parents {
				if err := step(); err != nil {
					return nil, nil, nil, err
				}
				edge := pathEdge{parent, name}
				final := i == len(parts)-1
				add(edge, final && (scope == nil || scope.matches(prefix, false)))
				for inode := range p.edges[edge] {
					if inode == 0 {
						continue
					}
					next[inode] = true
					if final && change.Recursive {
						if err := descend(inode, prefix, len(parts)); err != nil {
							return nil, nil, nil, err
						}
					}
				}
			}
			parents = next
		}
	}
	trace.detail(map[string]interface{}{"lookup_mode": "targeted", "scanned_edges": work, "visited_subtrees": len(visited)})
	return inodes, edges, retired, nil
}
