package vfs

import (
	"context"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

const directoryPositionDepth = 64

// beginDirectoryOperation 在缓存子项之前固定父关系，关闭回收与 inode 请求之间的窗口。
func (v *VFS) beginDirectoryOperation(ctx Context, directories ...Ino) (func(), syscall.Errno) {
	n := v.changes
	if n == nil {
		return func() {}, 0
	}
	n.Lock()
	var missing []Ino
	tracked := false
	for _, inode := range directories {
		if inode == rootID || IsSpecialNode(inode) {
			continue
		}
		tracked = true
		n.index.directoryPins[inode]++
		if len(n.index.parents[inode]) == 0 {
			missing = append(missing, inode)
		}
		// 回收可能已经取出快照；更新代次阻止它在打开或 lookup 完成后删除父关系。
		n.index.generation++
		for _, record := range n.index.parents[inode] {
			record.generation = n.index.generation
		}
	}
	n.Unlock()
	if !tracked {
		return func() {}, 0
	}
	release := func() {
		n.Lock()
		defer n.Unlock()
		for _, inode := range directories {
			if inode == rootID || IsSpecialNode(inode) {
				continue
			}
			n.index.directoryPins[inode]--
			if n.index.directoryPins[inode] == 0 {
				delete(n.index.directoryPins, inode)
				delete(n.index.directoryVersions, inode)
			}
		}
	}
	for _, inode := range missing {
		if st := v.positionDirectory(ctx, inode); st != 0 {
			release()
			return func() {}, st
		}
	}
	return release, 0
}

// positionDirectory 只补尚未缓存子项的当前目录，不能用当前路径掩盖历史定位缺口。
func (v *VFS) positionDirectory(ctx Context, inode Ino) (status syscall.Errno) {
	if inode == rootID || IsSpecialNode(inode) {
		return 0
	}
	n := v.changes
	n.Lock()
	if len(n.index.parents[inode]) > 0 {
		n.Unlock()
		return 0
	}
	if n.index.children[inode] > 0 || n.index.incomplete {
		n.Unlock()
		return syscall.EAGAIN
	}
	n.Unlock()
	started := time.Now()
	reason := "metadata_lookup"
	queries := 0
	depth := 0
	n.parentRepairAttempts.Add(1)
	defer func() {
		n.parentRepairNanos.Add(uint64(time.Since(started)))
		if status != 0 {
			n.parentRepairFailures.Add(1)
		}
		n.Lock()
		n.lastParentRepairInode, n.lastParentRepairErrno = inode, status
		if status != 0 {
			n.lastParentRepairFailure = map[string]interface{}{"inode": inode, "errno": int(status), "reason": reason, "queries": queries, "depth": depth, "duration_ms": float64(time.Since(started)) / 1e6, "unix_time": time.Now().Unix(), "request_pid": ctx.Pid(), "request_uid": ctx.Uid()}
		}
		n.Unlock()
	}()
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	lookupCtx := meta.WrapContext(deadline)
	type relation struct {
		inode  Ino
		parent Ino
		name   string
	}
	var chain []relation
	seen := make(map[Ino]bool)
	versions := make(map[Ino]uint64)
	defer func() {
		n.Lock()
		defer n.Unlock()
		for current := range versions {
			n.index.directoryPins[current]--
			if n.index.directoryPins[current] == 0 {
				delete(n.index.directoryPins, current)
				delete(n.index.directoryVersions, current)
			}
		}
	}()
	for current := inode; current != rootID; {
		if deadline.Err() != nil {
			reason = "deadline"
			return syscall.EAGAIN
		}
		if len(chain) >= directoryPositionDepth || seen[current] {
			reason = "depth_or_cycle"
			return syscall.EAGAIN
		}
		seen[current] = true
		n.Lock()
		n.index.directoryPins[current]++
		versions[current] = n.index.directoryVersions[current]
		known := len(n.index.parents[current]) > 0
		if known {
			_, complete := n.index.paths(current, false)
			n.Unlock()
			if !complete {
				reason = "incomplete_anchor"
				return syscall.EAGAIN
			}
			break
		}
		if n.index.children[current] > 0 || n.index.incomplete {
			n.Unlock()
			reason = "historical_gap"
			return syscall.EAGAIN
		}
		n.Unlock()
		queries++
		parent, name, st := v.Meta.GetDirParent(lookupCtx, current)
		if st != 0 {
			if deadline.Err() != nil {
				reason = "deadline"
			}
			return st
		}
		if parent == 0 || IsSpecialNode(parent) || name == "" || len(name) > meta.MaxName || strings.ContainsAny(name, "/\x00") || name == "." || name == ".." {
			reason = "invalid_parent"
			return syscall.EAGAIN
		}
		chain = append(chain, relation{current, parent, name})
		depth = len(chain)
		current = parent
	}
	// 两次读取必须一致；只核验本条父链的代次，无关目录写入不能干扰恢复。
	for _, item := range chain {
		queries++
		parent, name, st := v.Meta.GetDirParent(lookupCtx, item.inode)
		if st != 0 {
			if deadline.Err() != nil {
				reason = "deadline"
			}
			return st
		}
		if parent != item.parent || name != item.name || deadline.Err() != nil {
			reason = "concurrent_change"
			if deadline.Err() != nil {
				reason = "deadline"
			}
			return syscall.EAGAIN
		}
	}
	n.Lock()
	defer n.Unlock()
	if n.index.incomplete {
		reason = "capacity"
		return syscall.ENOBUFS
	}
	if len(n.index.parents[inode]) > 0 {
		// 并发读取可能已经补齐同一条父链；只复用完全相同且完整的结果。
		same := true
		for _, item := range chain {
			record := n.index.current[pathEdge{item.parent, item.name}]
			same = same && record != nil && record.inode == item.inode
		}
		_, currentComplete := n.index.paths(inode, false)
		_, historicalComplete := n.index.paths(inode, true)
		if same && currentComplete && historicalComplete {
			n.parentRepairSuccesses.Add(1)
			return 0
		}
		reason = "concurrent_change"
		return syscall.EAGAIN
	}
	if n.index.children[inode] > 0 {
		reason = "concurrent_change"
		return syscall.EAGAIN
	}
	for current, version := range versions {
		if n.index.directoryVersions[current] != version {
			reason = "concurrent_change"
			return syscall.EAGAIN
		}
	}
	// 整条父链一起提交，不向内核发通知，也不逐文件打印恢复日志。
	if n.index.size()+len(chain) > n.index.limit {
		reason = "capacity"
		return syscall.ENOBUFS
	}
	for i := len(chain) - 1; i >= 0; i-- {
		item := chain[i]
		n.rememberLocked(item.parent, item.name, item.inode, 1)
	}
	n.parentRepairSuccesses.Add(1)
	return 0
}
