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

type directoryPositionMeta struct {
	meta.Meta
	parent func(meta.Context, Ino) (Ino, string, syscall.Errno)
	lookup func(meta.Context, Ino, string, *Ino, *Attr, bool) syscall.Errno
}

func (m *directoryPositionMeta) GetDirParent(ctx meta.Context, inode Ino) (Ino, string, syscall.Errno) {
	return m.parent(ctx, inode)
}

func (m *directoryPositionMeta) Lookup(ctx meta.Context, parent Ino, name string, inode *Ino, attr *Attr, check bool) syscall.Errno {
	if m.lookup != nil {
		return m.lookup(ctx, parent, name, inode, attr, check)
	}
	return m.Meta.Lookup(ctx, parent, name, inode, attr, check)
}

func TestDirectoryPositionRestoredBeforeCachingChildAfterReclaim(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	agency, st := v.Mkdir(ctx, rootID, "position-agency", 0755, 0)
	require.Zero(t, st)
	dir, st := v.Mkdir(ctx, agency.Inode, "skill", 0755, 0)
	require.Zero(t, st)
	for v.collectChangeIndex(time.Now().Add(changeIndexIdle)) > 0 {
	}
	require.Empty(t, v.changes.index.parents)
	var file Ino
	require.Zero(t, v.Meta.Mknod(ctx, dir.Inode, "data", meta.TypeFile, 0644, 0, 0, "", &file, &Attr{}))
	entry, st := v.Lookup(ctx, dir.Inode, "data")
	require.Zero(t, st)
	require.Equal(t, file, entry.Inode)
	paths, complete := v.changes.index.paths(file, true)
	require.True(t, complete)
	require.Equal(t, []string{"/position-agency/skill/data"}, paths)
	v.changes.remember(rootID, "unrelated", 900001)
	invalidated := make(map[Ino]bool)
	v.InvalidateInode = func(inode Ino) syscall.Errno { invalidated[inode] = true; return 0 }
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/position-agency/skill/data"}}))
	require.Equal(t, map[Ino]bool{rootID: true, agency.Inode: true, dir.Inode: true, file: true}, invalidated)
	require.Equal(t, uint64(1), v.changes.parentRepairSuccesses.Load())
	require.Empty(t, v.changes.index.directoryPins)
}

func TestDirectoryPositionOpenDuringReclaimKeepsParentReceipt(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	dir, st := v.Mkdir(ctx, rootID, "position-open", 0755, 0)
	require.Zero(t, st)
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	v.InvalidateInode = func(inode Ino) syscall.Errno {
		if inode == dir.Inode {
			once.Do(func() { close(started); <-resume })
		}
		return 0
	}
	done := make(chan struct{})
	go func() { defer close(done); v.collectChangeIndex(time.Now().Add(changeIndexIdle)) }()
	<-started
	handle, st := v.Opendir(ctx, dir.Inode, syscall.O_RDONLY)
	require.Zero(t, st)
	close(resume)
	<-done
	require.Contains(t, v.changes.index.parents, dir.Inode)
	v.Releasedir(ctx, dir.Inode, handle)
	require.Eventually(t, func() bool { return len(v.findAllHandles(dir.Inode)) == 0 }, time.Second, time.Millisecond)
	require.Positive(t, v.collectChangeIndex(time.Now().Add(changeIndexIdle)))
}

func TestDirectoryPositionLookupPinsParentUntilChildRecorded(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	dir, st := v.Mkdir(ctx, rootID, "position-lookup", 0755, 0)
	require.Zero(t, st)
	backend := v.Meta
	started, resume := make(chan struct{}), make(chan struct{})
	v.Meta = &directoryPositionMeta{Meta: backend, lookup: func(ctx meta.Context, parent Ino, name string, inode *Ino, attr *Attr, check bool) syscall.Errno {
		close(started)
		<-resume
		return backend.Lookup(ctx, parent, name, inode, attr, check)
	}}
	done := make(chan syscall.Errno, 1)
	go func() { _, err := v.Lookup(ctx, dir.Inode, "absent"); done <- err }()
	<-started
	require.Zero(t, v.collectChangeIndex(time.Now().Add(changeIndexIdle)))
	close(resume)
	require.Equal(t, syscall.ENOENT, <-done)
	paths, complete := v.changes.index.paths(dir.Inode, true)
	require.True(t, complete)
	require.Equal(t, []string{"/position-lookup"}, paths)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/position-lookup/absent"}}))
	require.Empty(t, v.changes.index.directoryPins)
}

func TestDirectoryPositionNeverInventsHistoricalParent(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	dir, st := v.Mkdir(ctx, rootID, "position-history", 0755, 0)
	require.Zero(t, st)
	for v.collectChangeIndex(time.Now().Add(changeIndexIdle)) > 0 {
	}
	v.changes.remember(dir.Inode, "old-cached", 900002)
	_, st = v.Lookup(ctx, dir.Inode, "absent")
	require.Equal(t, syscall.EAGAIN, st)
	require.Zero(t, v.changes.parentRepairAttempts.Load())
	require.NotContains(t, v.changes.index.parents, dir.Inode)
	var calls int
	v.InvalidateInode = func(Ino) syscall.Errno { calls++; return 0 }
	v.InvalidateEntry = func(Ino, string) syscall.Errno { calls++; return 0 }
	require.ErrorIs(t, v.InvalidatePaths([]FileChange{{Path: "/position-history", Recursive: true}}), syscall.EAGAIN)
	require.Zero(t, calls)
	require.Empty(t, v.changes.index.directoryPins)
}

func TestDirectoryPositionRejectsConcurrentRenameWithoutPartialFill(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	dir, st := v.Mkdir(ctx, rootID, "position-before", 0755, 0)
	require.Zero(t, st)
	for v.collectChangeIndex(time.Now().Add(changeIndexIdle)) > 0 {
	}
	backend := v.Meta
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	v.Meta = &directoryPositionMeta{Meta: backend, parent: func(ctx meta.Context, inode Ino) (Ino, string, syscall.Errno) {
		parent, name, err := backend.GetDirParent(ctx, inode)
		once.Do(func() { close(started); <-resume })
		return parent, name, err
	}}
	done := make(chan syscall.Errno, 1)
	go func() { _, err := v.Lookup(ctx, dir.Inode, "absent"); done <- err }()
	<-started
	require.Zero(t, v.Rename(ctx, rootID, "position-before", rootID, "position-after", 0))
	close(resume)
	require.Equal(t, syscall.EAGAIN, <-done)
	require.Empty(t, v.changes.index.entries[dir.Inode])
	paths, complete := v.changes.index.paths(dir.Inode, false)
	require.True(t, complete)
	require.Equal(t, []string{"/position-after"}, paths)
	require.Equal(t, uint64(1), v.changes.parentRepairFailures.Load())
	require.Empty(t, v.changes.index.directoryPins)
}

func TestDirectoryPositionFailureIsBoundedAndRetryable(t *testing.T) {
	for _, mode := range []string{"cycle", "depth", "storage_error", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			v := changeGCFixture(t)
			ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
			calls := 0
			v.Meta = &directoryPositionMeta{Meta: v.Meta, parent: func(ctx meta.Context, inode Ino) (Ino, string, syscall.Errno) {
				calls++
				if mode == "storage_error" {
					return 0, "", syscall.EIO
				}
				if mode == "cycle" {
					return inode, "cycle", 0
				}
				return inode + 1, "deep", 0
			}}
			if mode == "cancelled" {
				ctx.Cancel()
			}
			_, st := v.Opendir(ctx, 800000, syscall.O_RDONLY)
			require.NotZero(t, st)
			require.LessOrEqual(t, calls, directoryPositionDepth)
			require.Empty(t, v.changes.index.edges)
			require.Empty(t, v.changes.index.directoryPins)
			require.Equal(t, uint64(1), v.changes.parentRepairFailures.Load())
		})
	}
}

func TestDirectoryPositionUnrelatedWritesDoNotAbortRepair(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	dir, st := v.Mkdir(ctx, rootID, "position-noise", 0755, 0)
	require.Zero(t, st)
	for v.collectChangeIndex(time.Now().Add(changeIndexIdle)) > 0 {
	}
	backend := v.Meta
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var queries int
	v.Meta = &directoryPositionMeta{Meta: backend, parent: func(ctx meta.Context, inode Ino) (Ino, string, syscall.Errno) {
		queries++
		parent, name, err := backend.GetDirParent(ctx, inode)
		once.Do(func() { close(started); <-resume })
		return parent, name, err
	}}
	done := make(chan syscall.Errno, 1)
	go func() { _, err := v.Lookup(ctx, dir.Inode, "absent"); done <- err }()
	<-started
	for i := 0; i < 10000; i++ {
		v.changes.remember(rootID, "noise-"+fmt.Sprint(i), Ino(900000+i))
	}
	close(resume)
	require.Equal(t, syscall.ENOENT, <-done)
	require.Equal(t, 2, queries)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/position-noise/absent"}}))
	require.Empty(t, v.changes.index.directoryPins)
	require.Empty(t, v.changes.index.directoryVersions)
}

func TestDirectoryPositionStorageFailureDoesNotPoisonRetry(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	dir, st := v.Mkdir(ctx, rootID, "position-retry", 0755, 0)
	require.Zero(t, st)
	for v.collectChangeIndex(time.Now().Add(changeIndexIdle)) > 0 {
	}
	backend := v.Meta
	v.Meta = &directoryPositionMeta{Meta: backend, parent: func(meta.Context, Ino) (Ino, string, syscall.Errno) { return 0, "", syscall.EIO }}
	_, st = v.Lookup(ctx, dir.Inode, "absent")
	require.Equal(t, syscall.EIO, st)
	require.Empty(t, v.changes.index.edges)
	require.Empty(t, v.changes.index.directoryPins)
	require.Empty(t, v.changes.index.directoryVersions)
	require.Equal(t, "metadata_lookup", v.changes.lastParentRepairFailure["reason"])
	require.Equal(t, dir.Inode, v.changes.lastParentRepairFailure["inode"])
	require.Equal(t, uint32(10), v.changes.lastParentRepairFailure["request_pid"])
	v.Meta = backend
	_, st = v.Lookup(ctx, dir.Inode, "absent")
	require.Equal(t, syscall.ENOENT, st)
	require.Equal(t, uint64(2), v.changes.parentRepairAttempts.Load())
	require.Equal(t, uint64(1), v.changes.parentRepairFailures.Load())
	require.Equal(t, uint64(1), v.changes.parentRepairSuccesses.Load())
}

func BenchmarkDirectoryPositionKnownParent(b *testing.B) {
	for _, size := range []int{100, 10000} {
		b.Run(fmt.Sprintf("index-%d", size), func(b *testing.B) {
			n := newChangeNotifier([]string{"skill/**"})
			for i := 0; i < size; i++ {
				n.remember(rootID, fmt.Sprintf("noise-%d", i), Ino(900000+i))
			}
			n.remember(rootID, "skill", 800000)
			v := &VFS{changes: n}
			ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				release, st := v.beginDirectoryOperation(ctx, 800000)
				if st != 0 {
					b.Fatal(st)
				}
				release()
			}
		})
	}
}

func TestDirectoryPositionConcurrentReadersShareIdenticalRepair(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	dir, st := v.Mkdir(ctx, rootID, "position-readers", 0755, 0)
	require.Zero(t, st)
	for v.collectChangeIndex(time.Now().Add(changeIndexIdle)) > 0 {
	}
	backend := v.Meta
	started, resume := make(chan struct{}, 2), make(chan struct{})
	var calls atomic.Int32
	v.Meta = &directoryPositionMeta{Meta: backend, parent: func(ctx meta.Context, inode Ino) (Ino, string, syscall.Errno) {
		parent, name, st := backend.GetDirParent(ctx, inode)
		if calls.Add(1) <= 2 {
			started <- struct{}{}
			<-resume
		}
		return parent, name, st
	}}
	done := make(chan syscall.Errno, 2)
	for i := 0; i < 2; i++ {
		go func() { _, st := v.Lookup(ctx, dir.Inode, "absent"); done <- st }()
	}
	<-started
	<-started
	close(resume)
	for i := 0; i < 2; i++ {
		require.Equal(t, syscall.ENOENT, <-done)
	}
	require.Equal(t, uint64(2), v.changes.parentRepairSuccesses.Load())
	require.Zero(t, v.changes.parentRepairFailures.Load())
	require.Empty(t, v.changes.index.directoryPins)
	require.Empty(t, v.changes.index.directoryVersions)
}

func TestDirectoryPositionAncestorLookupDoesNotAbortRepair(t *testing.T) {
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	parent, st := v.Mkdir(ctx, rootID, "position-anchor", 0755, 0)
	require.Zero(t, st)
	handle, st := v.Opendir(ctx, parent.Inode, syscall.O_RDONLY)
	require.Zero(t, st)
	t.Cleanup(func() { v.Releasedir(ctx, parent.Inode, handle) })
	dir, st := v.Mkdir(ctx, parent.Inode, "child", 0755, 0)
	require.Zero(t, st)
	require.Positive(t, v.collectChangeIndex(time.Now().Add(changeIndexIdle)))
	require.NotContains(t, v.changes.index.parents, dir.Inode)
	backend := v.Meta
	started, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	v.Meta = &directoryPositionMeta{Meta: backend, parent: func(ctx meta.Context, inode Ino) (Ino, string, syscall.Errno) {
		parent, name, st := backend.GetDirParent(ctx, inode)
		if calls.Add(1) == 2 {
			close(started)
			<-resume
		}
		return parent, name, st
	}}
	done := make(chan syscall.Errno, 1)
	go func() { _, err := v.Lookup(ctx, dir.Inode, "absent"); done <- err }()
	<-started
	_, st = v.Lookup(ctx, rootID, "position-anchor")
	require.Zero(t, st)
	close(resume)
	require.Equal(t, syscall.ENOENT, <-done)
	require.Zero(t, v.changes.parentRepairFailures.Load())
	require.Empty(t, v.changes.index.directoryPins)
}
