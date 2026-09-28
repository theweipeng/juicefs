package vfs

import (
	"fmt"
	"path"
	"strings"
	"syscall"
)

func validateChange(change FileChange) error {
	if !strings.HasPrefix(change.Path, "/") || strings.HasPrefix(change.Path, "//") || path.Clean(change.Path) != change.Path || strings.ContainsRune(change.Path, 0) {
		return fmt.Errorf("invalid volume path")
	}
	return nil
}

// InvalidatePaths 仅访问本机缓存索引，在控制连接的独立 goroutine 执行内核通知。
func (v *VFS) InvalidatePaths(changes []FileChange) (err error) {
	if v.changes != nil {
		defer func() {
			v.changes.invalidations.Add(1)
			if err != nil {
				v.changes.invalidationFailures.Add(1)
			}
		}()
	}
	if v.changes == nil || v.InvalidateEntry == nil || v.InvalidateInode == nil {
		return syscall.ENOTSUP
	}
	if len(changes) > 4096 {
		return syscall.E2BIG
	}
	for _, change := range changes {
		if err := validateChange(change); err != nil {
			return err
		}
		if change.OldPath != "" {
			old := FileChange{Path: change.OldPath, Recursive: change.Recursive}
			if err := validateChange(old); err != nil {
				return err
			}
			changes = append(changes, old)
		}
	}
	n := v.changes
	n.invalidationMu.Lock()
	defer n.invalidationMu.Unlock()
	inodes := make(map[Ino]bool)
	edges := make(map[pathEdge]bool)
	var retired []pathReceipt
	n.Lock()
	if n.index.incomplete {
		n.Unlock()
		return syscall.ENOBUFS
	}
	for edge, targets := range n.index.edges {
		parents, complete := n.index.paths(edge.parent, true)
		match := !complete
		for _, parent := range parents {
			for _, change := range changes {
				match = match || covers(change.Path, path.Join(parent, edge.name), change.Recursive)
			}
		}
		if match {
			edges[edge] = true
			inodes[edge.parent] = true
			for inode, record := range targets {
				if record.retired {
					retired = append(retired, pathReceipt{record, record.generation})
				}
				if inode != 0 {
					inodes[inode] = true
				}
			}
		}
	}
	// 未查找过的新条目也会改变父目录的 readdir 缓存。
	known := map[Ino]bool{rootID: true}
	for inode := range n.index.parents {
		known[inode] = true
	}
	for inode := range known {
		parentPaths, _ := n.index.paths(inode, true)
		for _, parentPath := range parentPaths {
			for _, change := range changes {
				prefix := strings.TrimSuffix(parentPath, "/") + "/"
				if strings.HasPrefix(change.Path, prefix) {
					relative := strings.TrimPrefix(change.Path, prefix)
					name := strings.SplitN(relative, "/", 2)[0]
					if name != "" {
						edges[pathEdge{inode, name}] = true
						inodes[inode] = true
					}
				}
			}
		}
	}
	n.Unlock()
	// 即使目标不存在，也失效已知父目录中的负目录项。
	for _, change := range changes {
		if change.Path == "/" {
			inodes[rootID] = true
			if change.Recursive {
				for _, inode := range v.Meta.CachedInodes() {
					inodes[inode] = true
				}
				v.hanleM.Lock()
				for inode := range v.handles {
					if !IsSpecialNode(inode) {
						inodes[inode] = true
					}
				}
				v.hanleM.Unlock()
			}
		}
	}
	if err := v.invalidateCachedPaths(inodes, edges); err != nil {
		return err
	}
	n.Lock()
	defer n.Unlock()
	if n.index.incomplete {
		return syscall.ENOBUFS
	}
	for _, receipt := range retired {
		n.index.remove(receipt)
	}
	return nil
}

// invalidateCachedPaths 不持有索引锁执行内核通知；调用方串行组织失效与回收。
func (v *VFS) invalidateCachedPaths(inodes map[Ino]bool, edges map[pathEdge]bool) error {
	for inode := range inodes {
		for _, h := range v.findAllHandles(inode) {
			h.Lock()
			writing := h.writer != nil
			h.Unlock()
			if writing {
				return syscall.EBUSY
			}
		}
	}
	var result error
	for inode := range inodes {
		v.Meta.InvalidateFileCache(inode)
		v.reader.InvalidateFile(inode)
		v.invalidateAttr(inode)
		for _, h := range v.findAllHandles(inode) {
			h.Lock()
			if h.dirHandler != nil {
				h.dirHandler.Close()
				h.dirHandler = nil
			}
			h.Unlock()
		}
		if err := v.InvalidateInode(inode); err != 0 && err != syscall.ENOENT {
			result = err
		}
	}
	for edge := range edges {
		if err := v.InvalidateEntry(edge.parent, edge.name); err != 0 && err != syscall.ENOENT {
			result = err
		}
	}
	return result
}
