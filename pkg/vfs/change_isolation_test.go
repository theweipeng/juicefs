package vfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

type changeIsolationScenario struct {
	Patterns         []string `json:"patterns"`
	WatchedFiles     []string `json:"watched_files"`
	NoiseDirectories []string `json:"noise_directories"`
}

func loadChangeIsolationScenario(t testing.TB) changeIsolationScenario {
	t.Helper()
	data, err := os.ReadFile("testdata/change_scope_isolation.json")
	require.NoError(t, err)
	var scenario changeIsolationScenario
	require.NoError(t, json.Unmarshal(data, &scenario))
	return scenario
}

// 真实 VFS 写入与提交，内核回调记录最终目标；不以 glob 单测替代范围验收。
func TestChangeIsolationHighNoiseWritesAndFinalInvalidationTargets(t *testing.T) {
	scenario := loadChangeIsolationScenario(t)
	v, _ := createTestVFS(func(conf *meta.Config) { conf.OpenCache = 180 * time.Second }, "")
	v.Meta.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return v.Store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	v.changes = newChangeNotifier(scenario.Patterns)
	v.writer.(*dataWriter).committed = v.notifyCommitted
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	known := map[string]Ino{"/": rootID}
	var directory func(string) Ino
	directory = func(value string) Ino {
		if inode, found := known[value]; found {
			return inode
		}
		parent := directory(path.Dir(value))
		entry, st := v.Mkdir(ctx, parent, path.Base(value), 0755, 0)
		require.Zero(t, st)
		known[value] = entry.Inode
		return entry.Inode
	}
	create := func(value, content string) (Ino, uint64) {
		entry, handle, st := v.Create(ctx, directory(path.Dir(value)), path.Base(value), 0644, 0, syscall.O_RDWR)
		require.Zero(t, st)
		require.Zero(t, v.Write(ctx, entry.Inode, []byte(content), 0, handle))
		require.Zero(t, v.Fsync(ctx, entry.Inode, 0, handle))
		known[value] = entry.Inode
		return entry.Inode, handle
	}
	closeHandle := func(inode Ino, handle uint64) {
		v.Release(ctx, inode, handle)
		require.Eventually(t, func() bool { return len(v.findAllHandles(inode)) == 0 }, time.Second, time.Millisecond)
	}
	for _, value := range scenario.WatchedFiles {
		inode, handle := create(value, "old")
		closeHandle(inode, handle)
	}
	for _, value := range scenario.NoiseDirectories {
		directory(value)
	}
	outsideWriter, writerHandle := create(scenario.NoiseDirectories[0]+"/busy", "unrelated writer")
	t.Cleanup(func() { v.Release(ctx, outsideWriter, writerHandle) })
	outsideReader, initialHandle := create(scenario.NoiseDirectories[1]+"/cached", "unrelated reader")
	closeHandle(outsideReader, initialHandle)
	_, readerHandle, st := v.Open(ctx, outsideReader, syscall.O_RDONLY)
	require.Zero(t, st)
	t.Cleanup(func() { v.Release(ctx, outsideReader, readerHandle) })
	data := make([]byte, 64)
	count, st := v.Read(ctx, outsideReader, data, 0, readerHandle)
	require.Zero(t, st)
	require.Equal(t, "unrelated reader", string(data[:count]))
	require.Contains(t, v.Meta.CachedInodes(), outsideReader)

	queue := make(chan changeFrame, 128)
	v.changes.subscribers[queue] = nil
	beforeInput, beforeFiltered := v.changes.inputChanges, v.changes.filteredChanges
	beforeSequence := v.changes.sequence
	started := time.Now()
	for index := 0; index < 128; index++ {
		for _, value := range scenario.NoiseDirectories {
			parent := directory(value)
			first, firstHandle := create(value+"/first", "noisy output")
			second, secondHandle := create(value+"/second", "other output")
			require.Zero(t, v.Rename(ctx, parent, "first", parent, "renamed", 0))
			require.Zero(t, v.Rename(ctx, parent, "renamed", parent, "second", meta.RenameExchange))
			closeHandle(first, firstHandle)
			closeHandle(second, secondHandle)
			require.Zero(t, v.Unlink(ctx, parent, "renamed"))
			require.Zero(t, v.Unlink(ctx, parent, "second"))
			_, st := v.Mkdir(ctx, parent, "left", 0755, 0)
			require.Zero(t, st)
			_, st = v.Mkdir(ctx, parent, "right", 0755, 0)
			require.Zero(t, st)
			require.Zero(t, v.Rename(ctx, parent, "left", parent, "right", meta.RenameExchange))
			require.Zero(t, v.Rmdir(ctx, parent, "left"))
			require.Zero(t, v.Rmdir(ctx, parent, "right"))
			require.Empty(t, queue)
		}
	}
	inputs := v.changes.inputChanges - beforeInput
	require.Greater(t, inputs, uint64(4000))
	require.Equal(t, inputs, v.changes.filteredChanges-beforeFiltered)
	require.Equal(t, beforeSequence, v.changes.sequence)
	require.Zero(t, v.changes.overflows)
	// 大量无关已定位关系不能影响定向查询的访问量，也不能补入读缓存或写句柄。
	for index := 0; index < 10000; index++ {
		v.changes.remember(directory(scenario.NoiseDirectories[2]), fmt.Sprintf("cached-%d", index), Ino(index+100000))
	}
	for _, value := range scenario.WatchedFiles {
		parent := known[path.Dir(value)]
		v.changes.remember(parent, "missing", 0)
	}
	for _, requested := range []string{"/", "/.yohu/agency/acme", "/.yohu/agency/acme/agents/alice", "/.yohu/agency/acme/agents/alice/skills/demo"} {
		t.Run(requested, func(t *testing.T) {
			inodes, entries := make(map[Ino]bool), make(map[pathEdge]bool)
			v.InvalidateInode = func(inode Ino) syscall.Errno { inodes[inode] = true; return 0 }
			v.InvalidateEntry = func(parent Ino, name string) syscall.Errno { entries[pathEdge{parent, name}] = true; return 0 }
			wantInodes, wantEntries := map[Ino]bool{rootID: true}, make(map[pathEdge]bool)
			for _, value := range scenario.WatchedFiles {
				if requested != "/" && !strings.HasPrefix(value, requested+"/") {
					continue
				}
				for prefix := value; prefix != "/"; prefix = path.Dir(prefix) {
					wantInodes[known[prefix]] = true
					wantEntries[pathEdge{known[path.Dir(prefix)], path.Base(prefix)}] = true
				}
				// 仅递归规则范围内的负目录项应被清理，配置文件同级负缓存不能扩大进来。
				if strings.HasSuffix(value, "/SKILL.md") || strings.HasSuffix(value, "/INDEX.md") {
					wantEntries[pathEdge{known[path.Dir(value)], "missing"}] = true
				}
			}
			trace := v.newInvalidationTrace("direct", "isolation", "", nil)
			err := v.invalidatePaths([]FileChange{{Path: requested, Recursive: true}}, trace, scenario.Patterns)
			trace.finish(err)
			require.NoError(t, err)
			require.Equal(t, wantInodes, inodes)
			require.Equal(t, wantEntries, entries)
			require.Less(t, trace.snapshot()["scanned_edges"].(int), 100)
			require.Nil(t, trace.snapshot()["position_checked_parents"])
		})
	}
	// 关注目录只改一次，必须有提交广播；无关写句柄仍不会让目标读取失效失败。
	skillPath := scenario.WatchedFiles[3]
	_, handle, st := v.Open(ctx, known[skillPath], syscall.O_RDWR)
	require.Zero(t, st)
	require.Zero(t, v.Write(ctx, known[skillPath], []byte("new"), 0, handle))
	require.Zero(t, v.Fsync(ctx, known[skillPath], 0, handle))
	closeHandle(known[skillPath], handle)
	frame := <-queue
	require.Equal(t, []FileChange{{Path: skillPath, Operation: "written"}}, frame.Changes)
	require.Empty(t, queue)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: skillPath}}))
	_, handle, st = v.Open(ctx, known[skillPath], syscall.O_RDONLY)
	require.Zero(t, st)
	count, st = v.Read(ctx, known[skillPath], data, 0, handle)
	require.Zero(t, st)
	require.Equal(t, "new", string(data[:count]))
	closeHandle(known[skillPath], handle)
	t.Logf("noise inputs=%d filtered=%d broadcasts=0 elapsed=%s; watched commits=1 broadcasts=1", inputs, inputs, time.Since(started))
}

func BenchmarkChangeIsolationUnrelatedFsync(b *testing.B) {
	scenario := loadChangeIsolationScenario(b)
	for _, size := range []int{100, 10000} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("index-%d/enabled-%t", size, enabled), func(b *testing.B) {
				v, _ := createTestVFS(nil, "")
				if enabled {
					v.changes = newChangeNotifier(scenario.Patterns)
					v.writer.(*dataWriter).committed = v.notifyCommitted
					for index := 0; index < size; index++ {
						v.changes.remember(rootID, fmt.Sprintf("unrelated-%d", index), Ino(index+100000))
					}
				}
				ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
				entry, handle, st := v.Create(ctx, rootID, "unrelated-output", 0644, 0, syscall.O_RDWR)
				require.Zero(b, st)
				b.Cleanup(func() { v.Release(ctx, entry.Inode, handle) })
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					require.Zero(b, v.Write(ctx, entry.Inode, []byte("unrelated contents"), 0, handle))
					require.Zero(b, v.Fsync(ctx, entry.Inode, 0, handle))
				}
				b.StopTimer()
				if enabled {
					require.Zero(b, v.changes.sequence)
					b.ReportMetric(float64(v.changes.matchingNanos)/float64(b.N), "match-ns/op")
				}
			})
		}
	}
}

func TestChangeIsolationRenameAndExchangeSelectOnlyRelevantHistory(t *testing.T) {
	for _, operation := range []string{"move_in", "move_out", "file_exchange", "directory_exchange"} {
		t.Run(operation, func(t *testing.T) {
			v := changeGCFixture(t)
			v.changes = newChangeNotifier([]string{"skill/**"})
			ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
			skill, st := v.Mkdir(ctx, rootID, "skill", 0755, 0)
			require.Zero(t, st)
			outside, st := v.Mkdir(ctx, rootID, "outside", 0755, 0)
			require.Zero(t, st)
			create := func(parent Ino, name string) Ino {
				entry, handle, st := v.Create(ctx, parent, name, 0644, 0, syscall.O_RDWR)
				require.Zero(t, st)
				v.Release(ctx, entry.Inode, handle)
				require.Eventually(t, func() bool { return len(v.findAllHandles(entry.Inode)) == 0 }, time.Second, time.Millisecond)
				return entry.Inode
			}
			busy, handle, st := v.Create(ctx, outside.Inode, "busy", 0644, 0, syscall.O_RDWR)
			require.Zero(t, st)
			t.Cleanup(func() { v.Release(ctx, busy.Inode, handle) })
			want := map[Ino]bool{rootID: true, skill.Inode: true}
			var oldParent, newParent Ino
			var flags uint32
			if operation == "directory_exchange" {
				left, st := v.Mkdir(ctx, skill.Inode, "package", 0755, 0)
				require.Zero(t, st)
				right, st := v.Mkdir(ctx, outside.Inode, "package", 0755, 0)
				require.Zero(t, st)
				want[left.Inode], want[right.Inode] = true, true
				want[create(left.Inode, "cached")], want[create(right.Inode, "cached")] = true, true
				oldParent, newParent, flags = skill.Inode, outside.Inode, meta.RenameExchange
			} else {
				if operation != "move_in" {
					want[create(skill.Inode, "package")] = true
				}
				if operation != "move_out" {
					want[create(outside.Inode, "package")] = true
				}
				oldParent, newParent = skill.Inode, outside.Inode
				if operation == "move_in" {
					oldParent, newParent = newParent, oldParent
				} else if operation == "file_exchange" {
					flags = meta.RenameExchange
				}
			}
			queue := make(chan changeFrame, 1)
			v.changes.subscribers[queue] = nil
			require.Zero(t, v.Rename(ctx, oldParent, "package", newParent, "package", flags))
			frame := <-queue
			require.Len(t, frame.Changes, 1)
			require.Equal(t, operation == "directory_exchange", frame.Changes[0].Recursive)
			require.False(t, frame.Resync)
			require.Empty(t, queue)
			actual, entries := make(map[Ino]bool), make(map[pathEdge]bool)
			v.InvalidateInode = func(inode Ino) syscall.Errno { actual[inode] = true; return 0 }
			v.InvalidateEntry = func(parent Ino, name string) syscall.Errno { entries[pathEdge{parent, name}] = true; return 0 }
			trace := v.newInvalidationTrace("direct", "cross-range", "", frame.Changes)
			err := v.invalidatePaths(frame.Changes, trace, []string{"skill/**"})
			trace.finish(err)
			require.NoError(t, err)
			require.Equal(t, want, actual)
			require.False(t, entries[pathEdge{outside.Inode, "package"}])
			require.False(t, entries[pathEdge{outside.Inode, "busy"}])
			// 已移动到配置外的旧目标仍属于相关历史缓存，不等同于无关文件。
			if operation == "directory_exchange" {
				require.NotNil(t, trace.snapshot()["position_checked_parents"])
			}
		})
	}
}

func TestChangeIsolationHardLinkUsesSharedInodeWithoutSelectingOtherParent(t *testing.T) {
	v := changeGCFixture(t)
	v.changes = newChangeNotifier([]string{"skill/**"})
	v.writer.(*dataWriter).committed = v.notifyCommitted
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	skill, st := v.Mkdir(ctx, rootID, "skill", 0755, 0)
	require.Zero(t, st)
	file, handle, st := v.Create(ctx, skill.Inode, "data", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	v.Release(ctx, file.Inode, handle)
	require.Eventually(t, func() bool { return len(v.findAllHandles(file.Inode)) == 0 }, time.Second, time.Millisecond)
	_, st = v.Link(ctx, file.Inode, rootID, "outside-alias")
	require.Zero(t, st)
	_, handle, st = v.Open(ctx, file.Inode, syscall.O_RDWR)
	require.Zero(t, st)
	queue := make(chan changeFrame, 1)
	v.changes.subscribers[queue] = nil
	require.Zero(t, v.Write(ctx, file.Inode, []byte("same inode"), 0, handle))
	require.Zero(t, v.Fsync(ctx, file.Inode, 0, handle))
	require.Equal(t, []FileChange{{Path: "/skill/data", Operation: "written"}}, (<-queue).Changes)
	require.ErrorIs(t, v.InvalidatePaths([]FileChange{{Path: "/skill/data"}}), syscall.EBUSY)
	v.Release(ctx, file.Inode, handle)
	require.Eventually(t, func() bool { return len(v.findAllHandles(file.Inode)) == 0 }, time.Second, time.Millisecond)
	entries := make(map[pathEdge]bool)
	v.InvalidateEntry = func(parent Ino, name string) syscall.Errno { entries[pathEdge{parent, name}] = true; return 0 }
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/skill/data"}}))
	require.False(t, entries[pathEdge{rootID, "outside-alias"}])
}

func TestChangeIsolationDirectoryHistoryChecksOnlyAffectedParentChains(t *testing.T) {
	v := changeGCFixture(t)
	for index := 0; index < 10000; index++ {
		parent := Ino(index + 100000)
		v.changes.remember(rootID, fmt.Sprintf("noise-%d", index), parent)
		v.changes.remember(parent, "cached", Ino(index+200000))
	}
	v.changes.remember(rootID, "old", 10)
	v.changes.remember(10, "cached", 11)
	v.changes.remember(rootID, "skill", 20)
	v.notifyRename(rootID, "old", rootID, "new", 10, true, 0)
	trace := v.newInvalidationTrace("direct", "history-cost", "", nil)
	err := v.invalidatePaths([]FileChange{{Path: "/skill", Recursive: true}}, trace)
	trace.finish(err)
	require.NoError(t, err)
	require.LessOrEqual(t, trace.snapshot()["position_checked_parents"].(int), 2)
	require.LessOrEqual(t, trace.snapshot()["scanned_edges"].(int), 2)
}

func TestChangeIsolationUnresolvedDescendantFailsBeforeCacheMutation(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.index.remember(rootID, "partial", 10, 2)
	v.changes.remember(10, "branch", 11)
	v.changes.remember(11, "cached", 12)
	v.changes.remember(rootID, "skill", 20)
	var calls int
	v.InvalidateInode = func(Ino) syscall.Errno { calls++; return 0 }
	v.InvalidateEntry = func(Ino, string) syscall.Errno { calls++; return 0 }
	for _, request := range []FileChange{{Path: "/skill", Recursive: true}, {Path: "/partial/branch"}} {
		trace := v.newInvalidationTrace("direct", "unresolved", "", nil)
		err := v.invalidatePaths([]FileChange{request}, trace)
		trace.finish(err)
		require.ErrorIs(t, err, syscall.EAGAIN)
		require.Zero(t, calls)
		require.Equal(t, "index_scope", trace.snapshot()["failure_stage"])
	}
	trace := v.newInvalidationTrace("direct", "unresolved-scoped-root", "", nil)
	err := v.invalidatePaths([]FileChange{{Path: "/", Recursive: true}}, trace, []string{"skill/**"})
	trace.finish(err)
	require.ErrorIs(t, err, syscall.EAGAIN)
	require.Zero(t, calls)
	// 只补全缺失的父关系，不需要用宽范围失效修复定位。
	v.changes.index.remember(rootID, "alias", 10, 2)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/skill", Recursive: true}}))
	require.Positive(t, calls)
}

func BenchmarkChangeIsolationCommittedFilter(b *testing.B) {
	scenario := loadChangeIsolationScenario(b)
	for _, size := range []int{100, 10000} {
		b.Run(fmt.Sprintf("index-%d", size), func(b *testing.B) {
			v, _ := createTestVFS(nil, "")
			v.changes = newChangeNotifier(scenario.Patterns)
			parent := Ino(rootID)
			for index, name := range strings.Split(strings.TrimPrefix(scenario.NoiseDirectories[0], "/"), "/") {
				inode := Ino(index + 10)
				v.changes.remember(parent, name, inode)
				parent = inode
			}
			v.changes.remember(parent, "output", 100)
			for index := 0; index < size; index++ {
				v.changes.remember(parent, fmt.Sprintf("cached-%d", index), Ino(index+100000))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				v.notifyCommitted(100)
			}
			b.StopTimer()
			require.Zero(b, v.changes.sequence)
			require.Equal(b, uint64(b.N), v.changes.filteredChanges)
		})
	}
}
