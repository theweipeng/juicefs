package vfs

import (
	"encoding/json"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestChangeGlob(t *testing.T) {
	data, err := os.ReadFile("testdata/change_globs.json")
	require.NoError(t, err)
	var cases []struct {
		Pattern string
		Path    string
		Matched bool
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	for _, tc := range cases {
		t.Run(tc.Pattern+":"+tc.Path, func(t *testing.T) {
			require.NoError(t, ValidateChangePatterns([]string{tc.Pattern}))
			n := newChangeNotifier([]string{tc.Pattern})
			queue := make(chan changeFrame, 1)
			n.subscribers[queue] = nil
			n.emitLocked([]FileChange{{Path: tc.Path}}, false)
			require.Equal(t, tc.Matched, len(queue) == 1)
		})
	}
}

func TestChangeGlobValidation(t *testing.T) {
	for _, pattern := range []string{"", "**/[abc", "**/{a,b", "**/file\\", "**/[]", "**/a\x00b", "**/\xff", strings.Repeat("a", 4097)} {
		require.Error(t, ValidateChangePatterns([]string{pattern}), "%q", pattern)
	}
	require.NoError(t, ValidateChangePatterns(nil))
	require.Error(t, ValidateChangePatterns(make([]string, 257)))
}

func TestChangeIndexReplacementAndNegativeEntry(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	v.changes = newChangeNotifier([]string{"**"})
	v.changes.remember(rootID, "file", 10)
	v.changes.remember(rootID, "file", 11)
	v.changes.remember(rootID, "missing", 0)
	invalidated := map[Ino]bool{}
	entries := map[string]bool{}
	v.InvalidateInode = func(inode Ino) syscall.Errno { invalidated[inode] = true; return 0 }
	v.InvalidateEntry = func(parent Ino, name string) syscall.Errno { entries[name] = true; return 0 }
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/file"}, {Path: "/missing"}}))
	require.True(t, invalidated[10])
	require.True(t, invalidated[11])
	require.True(t, entries["missing"])
	require.Error(t, v.InvalidatePaths([]FileChange{{Path: "/a/../b"}}))
}

func TestCommittedWriteAndNonBlockingOverflow(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	v.changes = newChangeNotifier([]string{"**"})
	v.writer.(*dataWriter).committed = v.notifyCommitted
	queue := make(chan changeFrame, 1)
	v.changes.subscribers[queue] = nil
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	entry, handle, st := v.Create(ctx, rootID, "skill", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	<-queue
	require.Zero(t, v.Write(ctx, entry.Inode, []byte("new content"), 0, handle))
	select {
	case <-queue:
		t.Fatal("buffered write emitted before commit")
	default:
	}
	require.Zero(t, v.Fsync(ctx, entry.Inode, 0, handle))
	select {
	case frame := <-queue:
		require.Equal(t, "written", frame.Changes[0].Operation)
	case <-time.After(time.Second):
		t.Fatal("missing committed change")
	}
	for i := 0; i < 10000; i++ {
		v.notifyCommitted(entry.Inode)
	}
	require.True(t, (<-queue).Resync)
	v.Release(ctx, entry.Inode, handle)
}

func TestInvalidationProtectsWritableHandles(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	v.changes = newChangeNotifier([]string{"**"})
	v.InvalidateInode = func(Ino) syscall.Errno { return 0 }
	v.InvalidateEntry = func(Ino, string) syscall.Errno { return 0 }
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	entry, handle, st := v.Create(ctx, rootID, "skill", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	require.ErrorIs(t, v.InvalidatePaths([]FileChange{{Path: "/skill"}}), syscall.EBUSY)
	v.Release(ctx, entry.Inode, handle)
}
