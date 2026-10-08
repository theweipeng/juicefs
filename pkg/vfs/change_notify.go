package vfs

import (
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"

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
	Errno        int          `json:"errno,omitempty"`
	FailureStage string       `json:"failure_stage,omitempty"`
	Resync       bool         `json:"resync,omitempty"`
}

type changeNotifier struct {
	sync.Mutex
	invalidationMu          sync.Mutex
	diagnostics             *invalidationDiagnostics
	reclaimWake             chan struct{}
	index                   *pathIndex
	patterns                []string
	matcher                 *changeMatcher
	epoch                   string
	origin                  string
	sequence                uint64
	overflows               uint64
	invalidations           atomic.Uint64
	invalidationFailures    atomic.Uint64
	reclaimFailures         atomic.Uint64
	parentRepairAttempts    atomic.Uint64
	parentRepairSuccesses   atomic.Uint64
	parentRepairFailures    atomic.Uint64
	parentRepairNanos       atomic.Uint64
	lastParentRepairInode   Ino
	lastParentRepairErrno   syscall.Errno
	lastParentRepairFailure map[string]interface{}
	subscribers             map[chan changeFrame]*changeMatcher
	inputChanges            uint64
	filteredChanges         uint64
	matchedChanges          uint64
	matchingNanos           uint64
	matchingMaxNanos        uint64
}

// ValidateChangePatterns 校验卷内 glob，不读取文件系统。
func ValidateChangePatterns(patterns []string) error {
	_, err := compileChangeMatcher(patterns)
	return err
}

func newChangeNotifier(patterns []string) *changeNotifier {
	n := &changeNotifier{index: newPathIndex(), origin: uuid.NewString(), epoch: uuid.NewString(), subscribers: make(map[chan changeFrame]*changeMatcher), reclaimWake: make(chan struct{}, 1)}
	n.diagnostics = newInvalidationDiagnostics()
	n.matcher, _ = compileChangeMatcher(patterns)
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
	if n.index.children[inode] > 0 && attr.Nlink != 1 {
		n.index.positionChecks[inode] = true
	}
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
	matched := make(map[chan changeFrame][]FileChange, len(n.subscribers))
	interesting := resync
	// 配置是自动通知的上限；订阅只能收窄，不能扩大配置范围。
	var configured []FileChange
	started := time.Now()
	n.inputChanges += uint64(len(changes))
	for _, change := range changes {
		if n.matcher.matchesChange(change) {
			configured = append(configured, change)
		}
	}
	elapsed := uint64(time.Since(started))
	n.matchingNanos += elapsed
	if elapsed > n.matchingMaxNanos {
		n.matchingMaxNanos = elapsed
	}
	n.matchedChanges += uint64(len(configured))
	n.filteredChanges += uint64(len(changes) - len(configured))
	interesting = interesting || len(configured) > 0
	if !interesting {
		return
	}
	for queue, additional := range n.subscribers {
		encodedBound := 1024
		for _, change := range configured {
			if additional == nil || len(additional.patterns) == 0 || additional.matchesChange(change) {
				matched[queue] = append(matched[queue], change)
				interesting = true
				encodedBound += 6*(len(change.Path)+len(change.OldPath)) + 128
				if len(matched[queue]) > 4096 || encodedBound > 1024*1024 {
					matched[queue] = nil
					resync = true
					break
				}
			}
		}
	}
	n.sequence++
	for queue := range n.subscribers {
		// 未命中的连接只接收空的序列进度帧，不能把过滤误判为丢包。
		frame := changeFrame{Type: "changed", Origin: n.origin, Epoch: n.epoch, Sequence: n.sequence, Changes: matched[queue], Resync: resync}
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

func (v *VFS) notifyRename(parent Ino, name string, newParent Ino, newName string, inode Ino, recursive bool, exchanged Ino) {
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
		n.rememberLocked(newParent, newName, inode)
		if exchanged != 0 {
			n.rememberLocked(parent, name, exchanged)
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
