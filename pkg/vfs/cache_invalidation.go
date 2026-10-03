package vfs

import (
	"fmt"
	"path"
	"strings"
	"syscall"
)

func validateChange(change FileChange) error {
	if !strings.HasPrefix(change.Path, "/") || strings.HasPrefix(change.Path, "//") || path.Clean(change.Path) != change.Path || strings.ContainsRune(change.Path, 0) {
		return fmt.Errorf("invalid volume path: %w", syscall.EINVAL)
	}
	return nil
}

// InvalidatePaths 仅访问本机缓存索引，在控制连接的独立 goroutine 执行内核通知。
func (v *VFS) InvalidatePaths(changes []FileChange) (err error) {
	trace := v.newInvalidationTrace("direct", "", "", changes)
	defer func() { trace.finish(err) }()
	return v.invalidatePaths(changes, trace)
}

func (v *VFS) invalidatePaths(changes []FileChange, trace *invalidationTrace, patterns ...[]string) (err error) {
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
	var scope *changeMatcher
	if len(patterns) > 0 {
		var compileErr error
		scope, compileErr = compileChangeMatcher(patterns[0])
		if compileErr != nil {
			return fmt.Errorf("invalid invalidation patterns: %w", syscall.EINVAL)
		}
		selected := make([]string, 0, 20)
		for _, pattern := range scope.patterns {
			if len(selected) == 20 {
				break
			}
			selected = append(selected, diagnosticText(pattern, 2048))
		}
		trace.detail(map[string]interface{}{"scoped": true, "scope_patterns": selected, "pattern_count": len(scope.patterns)})
	}
	changes = append([]FileChange(nil), changes...)
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
	if scope != nil {
		selected := changes[:0]
		for _, change := range changes {
			if scope.matches(change.Path, change.Recursive) {
				selected = append(selected, change)
			}
		}
		changes = selected
		if len(changes) == 0 {
			trace.detail(map[string]interface{}{"scope_filtered": true, "scanned_edges": 0, "matched_inodes": 0, "matched_entries": 0})
			return nil
		}
	}
	n := v.changes
	fullRoot := false
	for _, change := range changes {
		fullRoot = fullRoot || scope == nil && change.Path == "/" && change.Recursive
	}
	trace.phase("invalidation_lock", 0, pathEdge{})
	n.invalidationMu.Lock()
	trace.lockOwner(true)
	defer func() { trace.lockOwner(false); n.invalidationMu.Unlock() }()
	trace.phase("index_lock", 0, pathEdge{})
	n.Lock()
	if n.index.incomplete {
		n.Unlock()
		return syscall.ENOBUFS
	}
	trace.phase("index_match", 0, pathEdge{})
	trace.detail(map[string]interface{}{"index_edges": len(n.index.edges), "index_records": n.index.size(), "index_retired": n.index.retired.Len(), "index_limit": n.index.limit})
	inodes, edges, retired, matchErr := n.index.matchPaths(changes, scope, fullRoot, trace)
	n.Unlock()
	if matchErr != nil {
		return matchErr
	}
	// 只有显式全卷请求才能补入全部缓存；受规则裁剪的根路径沿用已匹配目标。
	trace.phase("cached_inode_collect", 0, pathEdge{})
	if fullRoot {
		inodes[rootID] = true
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
	trace.detail(map[string]interface{}{"matched_inodes": len(inodes), "matched_entries": len(edges), "retired_candidates": len(retired)})
	if err := v.invalidateCachedPaths(inodes, edges, trace); err != nil {
		return err
	}
	trace.phase("index_retire_lock", 0, pathEdge{})
	n.Lock()
	defer n.Unlock()
	if n.index.incomplete {
		return syscall.ENOBUFS
	}
	trace.phase("index_retire", 0, pathEdge{})
	reclaimed := 0
	for _, receipt := range retired {
		if n.index.remove(receipt) {
			reclaimed++
		}
	}
	trace.detail(map[string]interface{}{"reclaimed_records": reclaimed})
	return nil
}

// invalidateCachedPaths 不持有索引锁执行内核通知；调用方串行组织失效与回收。
func (v *VFS) invalidateCachedPaths(inodes map[Ino]bool, edges map[pathEdge]bool, trace *invalidationTrace) error {
	for inode := range inodes {
		trace.phase("busy_handles", inode, pathEdge{})
		for _, h := range v.findAllHandles(inode) {
			trace.detail(map[string]interface{}{"fh": h.fh})
			h.Lock()
			writing := h.writer != nil
			h.Unlock()
			if writing {
				trace.busy(v, inode, h, "writable_handle")
				trace.failure(syscall.EBUSY)
				return syscall.EBUSY
			}
		}
	}
	var result error
	completed := 0
	for inode := range inodes {
		trace.phase("metadata_cache", inode, pathEdge{})
		v.Meta.InvalidateFileCache(inode)
		trace.phase("read_buffers", inode, pathEdge{})
		v.reader.InvalidateFile(inode)
		trace.phase("attributes", inode, pathEdge{})
		v.invalidateAttr(inode)
		trace.phase("directory_handles", inode, pathEdge{})
		for _, h := range v.findAllHandles(inode) {
			h.Lock()
			if h.dirHandler != nil {
				h.dirHandler.Close()
				h.dirHandler = nil
			}
			h.Unlock()
		}
		trace.phase("kernel_inode", inode, pathEdge{})
		if err := v.InvalidateInode(inode); err != 0 && err != syscall.ENOENT {
			result = err
			trace.failure(err)
		}
		completed++
		trace.detail(map[string]interface{}{"completed_inodes": completed})
	}
	completed = 0
	for edge := range edges {
		trace.phase("kernel_entry", 0, edge)
		if err := v.InvalidateEntry(edge.parent, edge.name); err != 0 && err != syscall.ENOENT {
			result = err
			trace.failure(err)
		}
		completed++
		trace.detail(map[string]interface{}{"completed_entries": completed})
	}
	return result
}
