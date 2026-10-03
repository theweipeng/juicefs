package vfs

import (
	"syscall"
	"time"
)

// collectChangeIndex 只回收成功失效且期间未重新访问的关系，不扫描共享目录。
func (v *VFS) collectChangeIndex(now time.Time) (collected int) {
	n := v.changes
	if n == nil || v.InvalidateEntry == nil || v.InvalidateInode == nil {
		return
	}
	trace := v.newInvalidationTrace("reclaim", "", "", nil)
	defer func() { trace.finish(nil) }()
	trace.phase("invalidation_lock", 0, pathEdge{})
	n.invalidationMu.Lock()
	trace.lockOwner(true)
	defer func() { trace.lockOwner(false); n.invalidationMu.Unlock() }()
	trace.phase("index_lock", 0, pathEdge{})
	n.Lock()
	trace.phase("index_candidates", 0, pathEdge{})
	candidates := n.index.candidates(now)
	trace.detail(map[string]interface{}{"index_records": n.index.size(), "index_retired": n.index.retired.Len(), "retired_candidates": len(candidates)})
	n.Unlock()
	for _, receipt := range candidates {
		r := receipt.record
		// 活跃句柄仍可能通过 inode 写入；保留定位直到这些句柄释放。
		trace.phase("busy_handles", r.inode, r.edge)
		handles := v.findAllHandles(r.inode)
		busy := r.inode != 0 && len(handles) > 0
		var err error
		if busy {
			err = syscall.EBUSY
			// 活跃句柄是正常保留条件，记录计数和一个样例，不逐条告警。
			if trace.failureCount == 0 {
				trace.busy(v, r.inode, handles[0], "active_handle")
			}
			trace.failure(err)
		} else {
			inodes := map[Ino]bool{r.edge.parent: true}
			if r.inode != 0 {
				inodes[r.inode] = true
			}
			err = v.invalidateCachedPaths(inodes, map[pathEdge]bool{r.edge: true}, trace)
		}
		trace.phase("index_retire_lock", r.inode, r.edge)
		n.Lock()
		trace.phase("index_retire", r.inode, r.edge)
		if err == nil {
			if n.index.remove(receipt) {
				collected++
			}
		} else {
			n.index.retry(receipt)
		}
		n.Unlock()
		if err != nil {
			n.reclaimFailures.Add(1)
		}
	}
	trace.detail(map[string]interface{}{"reclaimed_records": collected})
	return collected
}

func (v *VFS) maintainChangeIndex(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-v.changes.reclaimWake:
		case <-stop:
			return
		}
		if v.collectChangeIndex(time.Now()) > 0 {
			// 有进展才继续清理积压；被活跃句柄或内核错误阻塞时等待下次唤醒。
			select {
			case v.changes.reclaimWake <- struct{}{}:
			default:
			}
		}
	}
}
