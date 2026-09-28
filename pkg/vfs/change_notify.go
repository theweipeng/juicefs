package vfs

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/google/uuid"
)

const ChangeProtocolVersion = 1

type FileChange struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
	Operation string `json:"operation"`
	OldPath   string `json:"old_path,omitempty"`
}

type changeFrame struct {
	Type         string       `json:"type"`
	ID           string       `json:"id,omitempty"`
	Protocol     int          `json:"protocol,omitempty"`
	Volume       string       `json:"volume,omitempty"`
	Origin       string       `json:"origin,omitempty"`
	Epoch        string       `json:"epoch,omitempty"`
	Sequence     uint64       `json:"sequence,omitempty"`
	Capabilities []string     `json:"capabilities,omitempty"`
	Changes      []FileChange `json:"changes,omitempty"`
	Patterns     []string     `json:"patterns,omitempty"`
	OK           bool         `json:"ok"`
	Error        string       `json:"error,omitempty"`
	Resync       bool         `json:"resync,omitempty"`
}

type changeNotifier struct {
	sync.Mutex
	invalidationMu       sync.Mutex
	reclaimWake          chan struct{}
	index                *pathIndex
	patterns             []string
	epoch                string
	origin               string
	sequence             uint64
	overflows            uint64
	invalidations        atomic.Uint64
	invalidationFailures atomic.Uint64
	reclaimFailures      atomic.Uint64
	subscribers          map[chan changeFrame]bool
}

// ValidateChangePatterns 校验卷内 glob，不读取文件系统。
func ValidateChangePatterns(patterns []string) error {
	if len(patterns) > 256 {
		return fmt.Errorf("too many change patterns (maximum 256)")
	}
	for _, pattern := range patterns {
		if pattern == "" || len(pattern) > 4096 || !utf8.ValidString(pattern) || strings.ContainsRune(pattern, 0) || !doublestar.ValidatePattern(pattern) {
			return fmt.Errorf("invalid change glob %q", pattern)
		}
	}
	return nil
}

func newChangeNotifier(patterns []string) *changeNotifier {
	n := &changeNotifier{index: newPathIndex(), origin: uuid.NewString(), epoch: uuid.NewString(), subscribers: make(map[chan changeFrame]bool), reclaimWake: make(chan struct{}, 1)}
	for _, pattern := range patterns {
		n.patterns = append(n.patterns, pattern)
	}
	return n
}

func (n *changeNotifier) remember(parent Ino, name string, inode Ino) {
	n.Lock()
	defer n.Unlock()
	n.rememberLocked(parent, name, inode)
}

func (n *changeNotifier) rememberEntry(parent Ino, name string, entry *meta.Entry) {
	links := entry.Attr.Nlink
	if entry.Attr.Typ == meta.TypeDirectory {
		links = 1
	}
	n.Lock()
	defer n.Unlock()
	n.rememberLocked(parent, name, entry.Inode, links)
}

// opened 让失效期间新建的句柄保留对应定位代次。
func (n *changeNotifier) opened(inode Ino, attr *Attr) {
	n.Lock()
	defer n.Unlock()
	if len(n.index.parents[inode]) == 0 {
		return
	}
	n.index.links[inode] = attr.Nlink
	n.index.generation++
	for _, record := range n.index.parents[inode] {
		record.generation = n.index.generation
	}
}

func (n *changeNotifier) rememberLocked(parent Ino, name string, inode Ino, links ...uint32) {
	if IsSpecialNode(parent) || IsSpecialNode(inode) {
		return
	}
	wasIncomplete := n.index.incomplete
	n.index.remember(parent, name, inode, links...)
	if n.index.retired.Len() > 0 || n.index.underPressure() {
		select {
		case n.reclaimWake <- struct{}{}:
		default:
		}
	}
	if !wasIncomplete && n.index.incomplete {
		logger.Errorf("file change path index reached capacity %d; invalidation is unavailable until remount", n.index.limit)
		n.emitLocked(nil, true)
	}
}

func (n *changeNotifier) emitLocked(changes []FileChange, resync bool) {
	resync = resync || n.index.incomplete
	var matched []FileChange
	encodedBound := 1024
	for _, change := range changes {
		match := change.Recursive
		for _, pattern := range n.patterns {
			if match {
				break
			}
			pattern = strings.TrimPrefix(pattern, "/")
			match = doublestar.MatchUnvalidated(pattern, strings.TrimPrefix(change.Path, "/")) ||
				change.OldPath != "" && doublestar.MatchUnvalidated(pattern, strings.TrimPrefix(change.OldPath, "/"))
		}
		if match {
			matched = append(matched, change)
			encodedBound += 6*(len(change.Path)+len(change.OldPath)) + 128
			if len(matched) > 4096 || encodedBound > 1024*1024 {
				matched = nil
				resync = true
				break
			}
		}
	}
	if len(matched) == 0 && !resync {
		return
	}
	n.sequence++
	frame := changeFrame{Type: "changed", Origin: n.origin, Epoch: n.epoch, Sequence: n.sequence, Changes: matched, Resync: resync}
	for queue := range n.subscribers {
		select {
		case queue <- frame:
		default:
			n.overflows++
			// 文件操作绝不等待消费者；缺口转换为范围重同步。
		drain:
			for {
				select {
				case <-queue:
				default:
					break drain
				}
			}
			queue <- changeFrame{Type: "resync_required", Origin: n.origin, Epoch: n.epoch, Sequence: n.sequence, Resync: true}
		}
	}
}

func (v *VFS) notifyRename(parent Ino, name string, newParent Ino, newName string, inode Ino, recursive bool, exchange ...bool) {
	n := v.changes
	if n == nil {
		return
	}
	n.Lock()
	defer n.Unlock()
	n.rememberLocked(parent, name, inode)
	oldParents, oldComplete := n.index.paths(parent, false)
	newParents, newComplete := n.index.paths(newParent, false)
	if parent != newParent || name != newName {
		var replaced Ino
		if previous := n.index.current[pathEdge{newParent, newName}]; previous != nil {
			replaced = previous.inode
		}
		n.rememberLocked(newParent, newName, inode)
		if len(exchange) > 0 && exchange[0] {
			n.rememberLocked(parent, name, replaced)
		} else {
			n.rememberLocked(parent, name, 0)
		}
	}
	var changes []FileChange
	for _, oldParent := range oldParents {
		for _, newParent := range newParents {
			if len(changes) >= 4096 {
				n.emitLocked(nil, true)
				return
			}
			changes = append(changes, FileChange{Path: path.Join(newParent, newName), OldPath: path.Join(oldParent, name), Recursive: recursive, Operation: "renamed"})
		}
	}
	n.emitLocked(changes, !oldComplete || !newComplete)
}

func (v *VFS) notifyEntry(parent Ino, name string, inode Ino, operation string, recursive bool) {
	n := v.changes
	if n == nil {
		return
	}
	n.Lock()
	defer n.Unlock()
	n.rememberLocked(parent, name, inode)
	parents, complete := n.index.paths(parent, false)
	var changes []FileChange
	for _, p := range parents {
		changes = append(changes, FileChange{Path: path.Join(p, name), Recursive: recursive, Operation: operation})
	}
	n.emitLocked(changes, !complete)
}

func (v *VFS) notifyCommitted(inode Ino) {
	n := v.changes
	if n == nil {
		return
	}
	n.Lock()
	defer n.Unlock()
	paths, complete := n.index.paths(inode, false)
	var changes []FileChange
	for _, p := range paths {
		changes = append(changes, FileChange{Path: p, Operation: "written"})
	}
	n.emitLocked(changes, !complete)
}
