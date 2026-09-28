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
	n.invalidationMu.Lock()
	defer n.invalidationMu.Unlock()
	n.Lock()
	candidates := n.index.candidates(now)
	n.Unlock()
	for _, receipt := range candidates {
		r := receipt.record
		// 活跃句柄仍可能通过 inode 写入；保留定位直到这些句柄释放。
		busy := r.inode != 0 && len(v.findAllHandles(r.inode)) > 0
		var err error
		if busy {
			err = syscall.EBUSY
		} else {
			inodes := map[Ino]bool{r.edge.parent: true}
			if r.inode != 0 {
				inodes[r.inode] = true
			}
			err = v.invalidateCachedPaths(inodes, map[pathEdge]bool{r.edge: true})
		}
		n.Lock()
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
