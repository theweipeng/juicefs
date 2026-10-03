package vfs

import (
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func changeGCFixture(t *testing.T) *VFS {
	t.Helper()
	v, _ := createTestVFS(nil, "")
	v.changes = newChangeNotifier([]string{"**"})
	v.InvalidateInode = func(Ino) syscall.Errno { return 0 }
	v.InvalidateEntry = func(Ino, string) syscall.Errno { return 0 }
	return v
}

func TestChangeIndexRetainsLastHistoricalParentUntilChildrenAreReclaimed(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, "unrelated", 10)
	v.changes.remember(10, "cached-child", 11)
	v.changes.remember(rootID, "skill", 20)
	v.changes.remember(rootID, "unrelated", 0)
	p := v.changes.index
	parent := p.edges[pathEdge{rootID, "unrelated"}][10]
	require.False(t, p.remove(pathReceipt{parent, parent.generation}))
	paths, complete := p.paths(10, true)
	require.True(t, complete)
	require.Equal(t, []string{"/unrelated"}, paths)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/skill", Recursive: true}}))
	child := p.current[pathEdge{10, "cached-child"}]
	require.True(t, p.remove(pathReceipt{child, child.generation}))
	require.True(t, p.remove(pathReceipt{parent, parent.generation}))
}

func TestChangeIndexReplacementHistoryReclaimed(t *testing.T) {
	v := changeGCFixture(t)
	var invalidated int
	v.InvalidateInode = func(Ino) syscall.Errno { invalidated++; return 0 }
	for i := 0; i < 10000; i++ {
		v.changes.remember(rootID, "config", Ino(i+10))
		if i%64 == 0 {
			v.collectChangeIndex(time.Now())
		}
	}
	for v.collectChangeIndex(time.Now()) > 0 {
	}
	p := v.changes.index
	require.Equal(t, 1, p.size())
	require.Len(t, p.parents, 1)
	require.Len(t, p.edges[pathEdge{rootID, "config"}], 1)
	require.Equal(t, uint64(9999), p.reclaimed)
	require.GreaterOrEqual(t, invalidated, 9999)
	require.False(t, p.incomplete)
}

func TestChangeIndexInvalidationFailureRetainsOldInode(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, "config", 10)
	v.changes.remember(rootID, "config", 11)
	v.InvalidateEntry = func(Ino, string) syscall.Errno { return syscall.EIO }
	require.Zero(t, v.collectChangeIndex(time.Now()))
	require.Len(t, v.changes.index.edges[pathEdge{rootID, "config"}], 2)
	require.ErrorIs(t, v.InvalidatePaths([]FileChange{{Path: "/config"}}), syscall.EIO)
	require.Len(t, v.changes.index.parents, 2)
	v.InvalidateEntry = func(Ino, string) syscall.Errno { return 0 }
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/config"}}))
	require.NotContains(t, v.changes.index.parents, Ino(10))
	require.Contains(t, v.changes.index.parents, Ino(11))
}

func TestChangeIndexConcurrentLookupKeepsNewReceipt(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, "config", 10)
	v.changes.remember(rootID, "config", 11)
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	v.InvalidateInode = func(ino Ino) syscall.Errno {
		if ino == 10 {
			once.Do(func() { close(started); <-resume })
		}
		return 0
	}
	done := make(chan struct{})
	go func() { defer close(done); v.collectChangeIndex(time.Now()) }()
	<-started
	// 不持有索引锁等待内核；重访的旧 inode 随后再次被替换，代次也必须保留。
	v.changes.remember(rootID, "config", 10)
	v.changes.remember(rootID, "config", 12)
	close(resume)
	<-done
	require.Contains(t, v.changes.index.parents, Ino(10))
	v.collectChangeIndex(time.Now())
	require.NotContains(t, v.changes.index.parents, Ino(10))
}

func TestChangeIndexPinnedHandlesRetriedAfterClose(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	entry, fh, st := v.Create(ctx, rootID, "config", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	v.changes.remember(rootID, "config", entry.Inode+100)
	require.Zero(t, v.collectChangeIndex(time.Now()))
	require.Contains(t, v.changes.index.parents, entry.Inode)
	v.Release(ctx, entry.Inode, fh)
	require.Eventually(t, func() bool { return len(v.findAllHandles(entry.Inode)) == 0 }, time.Second, time.Millisecond)
	require.Positive(t, v.collectChangeIndex(time.Now()))
	require.NotContains(t, v.changes.index.parents, entry.Inode)
}

func TestChangeIndexRenameAndHardLinkCompleteness(t *testing.T) {
	v := changeGCFixture(t)
	n := v.changes
	n.remember(rootID, "old", 10)
	n.remember(10, "file", 11)
	v.notifyRename(rootID, "old", rootID, "new", 10, true, 0)
	paths, complete := n.index.paths(11, false)
	require.True(t, complete)
	require.Equal(t, []string{"/new/file"}, paths)
	cached, _ := n.index.paths(11, true)
	require.ElementsMatch(t, []string{"/old/file", "/new/file"}, cached)
	v.collectChangeIndex(time.Now())
	cached, complete = n.index.paths(11, true)
	require.True(t, complete)
	require.Equal(t, []string{"/new/file"}, cached)

	n.index.remember(rootID, "a", 20, 2)
	n.index.remember(rootID, "b", 20, 2)
	r := n.index.current[pathEdge{rootID, "a"}]
	require.True(t, n.index.remove(pathReceipt{r, r.generation}))
	paths, complete = n.index.paths(20, false)
	require.Equal(t, []string{"/b"}, paths)
	require.False(t, complete, "回收了别名后不能把剩余一个当作完整集合")
	n.index.remember(rootID, "a", 20, 2)
	_, complete = n.index.paths(20, false)
	require.True(t, complete)
}

func TestChangeIndexTruncationAndCyclesRequireResync(t *testing.T) {
	v := changeGCFixture(t)
	q := make(chan changeFrame, 2)
	v.changes.subscribers[q] = nil
	for i := 0; i < 4097; i++ {
		v.changes.remember(rootID, fmt.Sprintf("f%04d", i), 10)
	}
	v.notifyCommitted(10)
	require.True(t, (<-q).Resync)
	p := newPathIndex()
	p.remember(rootID, "valid", 10)
	p.remember(11, "cycle", 10)
	p.remember(10, "back", 11)
	paths, complete := p.paths(10, false)
	require.Contains(t, paths, "/valid")
	require.False(t, complete)
}

func TestChangeIndexCapacityCannotReturnSuccessfulInvalidation(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.index.limit = 8
	for i := 0; i < 10000; i++ {
		v.changes.remember(rootID, fmt.Sprintf("name-%d", i), Ino(i+10))
	}
	require.Equal(t, 8, v.changes.index.size())
	require.True(t, v.changes.index.incomplete)
	require.ErrorIs(t, v.InvalidatePaths([]FileChange{{Path: "/", Recursive: true}}), syscall.ENOBUFS)
	v.collectChangeIndex(time.Now().Add(changeIndexIdle))
	require.Zero(t, v.changes.index.size())
	// 空间恢复不代表未记录的内核缓存已经被清理。
	require.ErrorIs(t, v.InvalidatePaths([]FileChange{{Path: "/", Recursive: true}}), syscall.ENOBUFS)
}

func TestChangeIndexIdleEntriesAndNegativeDentriesReclaimed(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, "dir", 10)
	v.changes.remember(10, "missing", 0)
	v.changes.remember(10, "file", 11)
	var removed []string
	v.InvalidateEntry = func(_ Ino, name string) syscall.Errno { removed = append(removed, name); return 0 }
	v.collectChangeIndex(time.Now().Add(changeIndexIdle))
	require.Contains(t, v.changes.index.parents, Ino(10), "父目录仍是本轮快照的定位基座")
	v.collectChangeIndex(time.Now().Add(changeIndexIdle))
	require.Zero(t, v.changes.index.size())
	require.Empty(t, v.changes.index.parents)
	require.Empty(t, v.changes.index.current)
	require.Empty(t, v.changes.index.children)
	require.Empty(t, v.changes.index.links)
	require.ElementsMatch(t, []string{"missing", "file", "dir"}, removed)
}

func TestChangeIndexConcurrentUpdatesInvalidationAndGC(t *testing.T) {
	v := changeGCFixture(t)
	var calls atomic.Int64
	v.InvalidateInode = func(Ino) syscall.Errno { calls.Add(1); return 0 }
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				name := fmt.Sprintf("config-%d", worker)
				v.changes.remember(rootID, name, Ino(worker*1000+i+10))
				if worker%2 == 0 {
					_ = v.InvalidatePaths([]FileChange{{Path: "/" + name}})
				} else {
					v.collectChangeIndex(time.Now())
				}
			}
		}(worker)
	}
	wg.Wait()
	for v.collectChangeIndex(time.Now()) > 0 {
	}
	require.Equal(t, 4, v.changes.index.size())
	require.False(t, v.changes.index.incomplete)
	require.Positive(t, calls.Load())
}

func TestChangeIndexDisabledDoesNotAllocateOrStartMaintenance(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	require.Nil(t, v.changes)
	require.Zero(t, v.collectChangeIndex(time.Now()))
}

func TestChangeIndexAtomicReplacementAndDirectoryExchange(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	var final Ino
	for i := 0; i < 200; i++ {
		entry, fh, st := v.Create(ctx, rootID, "replacement", 0644, 0, syscall.O_RDWR)
		require.Zero(t, st)
		require.Zero(t, v.Write(ctx, entry.Inode, []byte("new contents"), 0, fh))
		require.Zero(t, v.Fsync(ctx, entry.Inode, 0, fh))
		require.Zero(t, v.Rename(ctx, rootID, "replacement", rootID, "config", 0))
		v.Release(ctx, entry.Inode, fh)
		final = entry.Inode
		v.collectChangeIndex(time.Now())
	}
	require.Eventually(t, func() bool {
		v.collectChangeIndex(time.Now())
		v.changes.Lock()
		defer v.changes.Unlock()
		return v.changes.index.retired.Len() == 0
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, final, v.changes.index.current[pathEdge{rootID, "config"}].inode)
	require.Equal(t, 2, v.changes.index.size())

	left, st := v.Mkdir(ctx, rootID, "left", 0755, 0)
	require.Zero(t, st)
	right, st := v.Mkdir(ctx, rootID, "right", 0755, 0)
	require.Zero(t, st)
	require.Zero(t, v.Rename(ctx, rootID, "left", rootID, "right", meta.RenameExchange))
	leftPaths, complete := v.changes.index.paths(left.Inode, false)
	require.True(t, complete)
	require.Equal(t, []string{"/right"}, leftPaths)
	rightPaths, complete := v.changes.index.paths(right.Inode, false)
	require.True(t, complete)
	require.Equal(t, []string{"/left"}, rightPaths)
}

func TestChangeIndexDeepPartialPathsAreNotComplete(t *testing.T) {
	p := newPathIndex()
	p.remember(rootID, "valid", 500)
	parent := Ino(rootID)
	for ino := Ino(10); ino < 280; ino++ {
		p.remember(parent, "deep", ino)
		parent = ino
	}
	p.remember(parent, "other", 500)
	paths, complete := p.paths(500, false)
	require.Contains(t, paths, "/valid")
	require.False(t, complete)
}

func TestChangeIndexConcurrentChildPreservesAncestor(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, "dir", 10)
	v.InvalidateEntry = func(Ino, string) syscall.Errno {
		// 清理期间新增的子项必须仍有完整父目录链。
		v.changes.remember(10, "new", 11)
		return 0
	}
	require.Zero(t, v.collectChangeIndex(time.Now().Add(changeIndexIdle)))
	paths, complete := v.changes.index.paths(11, false)
	require.True(t, complete)
	require.Equal(t, []string{"/dir/new"}, paths)
}

func TestChangeIndexExcludesInternalNodes(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, ".stats", StatsInode)
	require.Zero(t, v.changes.index.size())
}
