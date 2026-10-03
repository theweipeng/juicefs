//go:build !windows

package vfs

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestChangeSubscriptionCannotEscapeConfiguredPatterns(t *testing.T) {
	n := newChangeNotifier([]string{"/skill/**"})
	queue := make(chan changeFrame, 4)
	n.subscribers[queue], _ = compileChangeMatcher([]string{"**"})
	n.emitLocked([]FileChange{{Path: "/_runtime/unrelated", Recursive: true}}, false)
	require.Empty(t, queue)
	require.Zero(t, n.sequence)
	n.emitLocked([]FileChange{{Path: "/skill/README.md"}}, false)
	require.Equal(t, "/skill/README.md", (<-queue).Changes[0].Path)
}

func TestChangeHealthDoesNotWaitForIndexAndIncludesDuration(t *testing.T) {
	v := changeGCFixture(t)
	trace := v.newInvalidationTrace("control", "health", "", nil)
	trace.finish(nil)
	values := changeHistogramSummary(v.changes.diagnostics.durations)
	require.Len(t, values, 1)
	require.Equal(t, uint64(1), values[0]["count"])
	require.Greater(t, values[0]["total_ms"].(float64), float64(0))
	v.changes.Lock()
	defer v.changes.Unlock()
	done := make(chan struct{})
	go func() { defer close(done); v.logChangeHealth() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health diagnostic waited for the index lock")
	}
}

func TestInvalidationTargetedLookupDoesNotScanUnrelatedIndex(t *testing.T) {
	v := changeGCFixture(t)
	for i := 0; i < 10000; i++ {
		v.changes.remember(rootID, fmt.Sprintf("unrelated-%d", i), Ino(i+100))
	}
	v.changes.remember(rootID, "skill", 10)
	v.changes.remember(10, "SKILL.md", 11)
	trace := v.newInvalidationTrace("direct", "targeted", "", nil)
	err := v.invalidatePaths([]FileChange{{Path: "/skill/SKILL.md"}}, trace)
	trace.finish(err)
	require.NoError(t, err)
	fields := trace.snapshot()
	require.Equal(t, "targeted", fields["lookup_mode"])
	require.LessOrEqual(t, fields["scanned_edges"].(int), 3)
	require.Nil(t, fields["position_checked_parents"])
}

func TestInvalidationScopedAncestorDoesNotEvictUnrelatedSubtree(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, "agency", 10)
	v.changes.remember(10, "skill", 11)
	v.changes.remember(11, "SKILL.md", 12)
	v.changes.remember(11, "absent", 0)
	v.changes.remember(10, "_runtime", 20)
	for i := 0; i < 10000; i++ {
		v.changes.remember(20, fmt.Sprintf("unrelated-%d", i), Ino(i+100))
	}
	invalidated := make(map[Ino]bool)
	edges := make(map[pathEdge]bool)
	v.InvalidateInode = func(inode Ino) syscall.Errno { invalidated[inode] = true; return 0 }
	v.InvalidateEntry = func(parent Ino, name string) syscall.Errno { edges[pathEdge{parent, name}] = true; return 0 }
	trace := v.newInvalidationTrace("direct", "scoped", "", nil)
	err := v.invalidatePaths([]FileChange{{Path: "/agency", Recursive: true}}, trace, []string{"agency/skill/**"})
	trace.finish(err)
	require.NoError(t, err)
	require.True(t, invalidated[12])
	require.True(t, edges[pathEdge{11, "absent"}])
	require.False(t, invalidated[20])
	require.Len(t, invalidated, 4)
	trace = v.newInvalidationTrace("direct", "unrelated", "", nil)
	err = v.invalidatePaths([]FileChange{{Path: "/agency/_runtime", Recursive: true}}, trace, []string{"agency/skill/**"})
	trace.finish(err)
	require.NoError(t, err)
	require.True(t, trace.snapshot()["scope_filtered"].(bool))
}

func TestInvalidationScopedRootDoesNotSelectUnrelatedCacheOrHandles(t *testing.T) {
	for _, mode := range []string{"writable", "read_only", "cached_only"} {
		t.Run(mode, func(t *testing.T) {
			v, _ := createTestVFS(func(conf *meta.Config) { conf.OpenCache = 180 * time.Second }, "")
			v.changes = newChangeNotifier([]string{"skill/**"})
			ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
			outside, handle, st := v.Create(ctx, rootID, "outside", 0644, 0, syscall.O_RDWR)
			require.Zero(t, st)
			if mode == "writable" {
				t.Cleanup(func() { v.Release(ctx, outside.Inode, handle) })
			} else {
				v.Release(ctx, outside.Inode, handle)
				require.Eventually(t, func() bool { return len(v.findAllHandles(outside.Inode)) == 0 }, time.Second, time.Millisecond)
				if mode == "read_only" {
					_, readHandle, openErr := v.Open(ctx, outside.Inode, syscall.O_RDONLY)
					require.Zero(t, openErr)
					t.Cleanup(func() { v.Release(ctx, outside.Inode, readHandle) })
				}
			}
			require.Contains(t, v.Meta.CachedInodes(), outside.Inode)
			skill, st := v.Mkdir(ctx, rootID, "skill", 0755, 0)
			require.Zero(t, st)
			content, contentHandle, st := v.Create(ctx, skill.Inode, "SKILL.md", 0644, 0, syscall.O_RDWR)
			require.Zero(t, st)
			v.Release(ctx, content.Inode, contentHandle)
			require.Eventually(t, func() bool { return len(v.findAllHandles(content.Inode)) == 0 }, time.Second, time.Millisecond)
			v.changes.remember(skill.Inode, "missing", 0)
			invalidated, entries := make(map[Ino]bool), make(map[pathEdge]bool)
			v.InvalidateInode = func(inode Ino) syscall.Errno { invalidated[inode] = true; return 0 }
			v.InvalidateEntry = func(parent Ino, name string) syscall.Errno { entries[pathEdge{parent, name}] = true; return 0 }
			changes := []FileChange{{Path: "/", Recursive: true}}
			trace := v.newInvalidationTrace("direct", "scoped-root", "", changes)
			err := v.invalidatePaths(changes, trace, []string{"skill/**"})
			trace.finish(err)
			require.NoError(t, err)
			require.Equal(t, map[Ino]bool{rootID: true, skill.Inode: true, content.Inode: true}, invalidated)
			require.Equal(t, map[pathEdge]bool{{rootID, "skill"}: true, {skill.Inode, "SKILL.md"}: true, {skill.Inode, "missing"}: true}, entries)
		})
	}
}

func TestInvalidationExplicitAPIIsIndependentOfNotificationGlobs(t *testing.T) {
	v := changeGCFixture(t)
	v.changes = newChangeNotifier([]string{"skill/**"})
	v.changes.remember(rootID, "outside", 10)
	v.changes.remember(10, "target", 11)
	v.changes.remember(10, "sibling", 12)
	invalidated := make(map[Ino]bool)
	v.InvalidateInode = func(inode Ino) syscall.Errno { invalidated[inode] = true; return 0 }
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/outside/target"}}))
	require.Equal(t, map[Ino]bool{rootID: true, 10: true, 11: true}, invalidated)
	clear(invalidated)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/outside"}}))
	require.Equal(t, map[Ino]bool{rootID: true, 10: true}, invalidated)
	clear(invalidated)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/outside", Recursive: true}}))
	require.Equal(t, map[Ino]bool{rootID: true, 10: true, 11: true, 12: true}, invalidated)
	clear(invalidated)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/"}}))
	require.Equal(t, map[Ino]bool{rootID: true}, invalidated)
	clear(invalidated)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/", Recursive: true}}))
	require.Equal(t, map[Ino]bool{rootID: true, 10: true, 11: true, 12: true}, invalidated)
}

func TestChangeRecursiveGlobIntersection(t *testing.T) {
	data, err := os.ReadFile("testdata/change_recursive_globs.json")
	require.NoError(t, err)
	var cases []struct {
		Pattern, Path string
		Matched       bool
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	for _, tc := range cases {
		t.Run(tc.Pattern+":"+tc.Path, func(t *testing.T) {
			matcher, err := compileChangeMatcher([]string{tc.Pattern})
			require.NoError(t, err)
			require.Equal(t, tc.Matched, matcher.matches(tc.Path, true))
			n := newChangeNotifier([]string{tc.Pattern})
			queue := make(chan changeFrame, 1)
			n.subscribers[queue] = nil
			n.emitLocked([]FileChange{{Path: tc.Path, Recursive: true, Operation: "created"}}, false)
			require.Equal(t, tc.Matched, len(queue) == 1)
		})
	}
}

func TestChangeEmptyPatternsDoNotBroadcastDirectories(t *testing.T) {
	n := newChangeNotifier(nil)
	queue := make(chan changeFrame, 1)
	n.subscribers[queue] = nil
	n.emitLocked([]FileChange{{Path: "/directory", Recursive: true}}, false)
	require.Empty(t, queue)
	require.Zero(t, n.sequence)
}

func TestChangeSubscriptionsIsolateAdditionalPatternsAndKeepSequence(t *testing.T) {
	n := newChangeNotifier([]string{"/base/**", "/first/**", "/second/**"})
	first, second := make(chan changeFrame, 8), make(chan changeFrame, 8)
	n.subscribers[first], _ = compileChangeMatcher([]string{"/base/**", "/first/**"})
	n.subscribers[second], _ = compileChangeMatcher([]string{"/base/**", "/second/**"})
	n.emitLocked([]FileChange{{Path: "/first/file"}}, false)
	a, b := <-first, <-second
	require.Equal(t, uint64(1), a.Sequence)
	require.Equal(t, a.Sequence, b.Sequence)
	require.Len(t, a.Changes, 1)
	require.Empty(t, b.Changes)
	n.emitLocked([]FileChange{{Path: "/base/file"}}, false)
	require.Len(t, (<-first).Changes, 1)
	require.Len(t, (<-second).Changes, 1)
	delete(n.subscribers, first)
	n.emitLocked([]FileChange{{Path: "/first/now-unwatched", Recursive: true}}, false)
	require.Empty(t, (<-second).Changes)
	require.Equal(t, uint64(3), n.sequence)
	require.Equal(t, []string{"/base/**", "/first/**", "/second/**"}, n.patterns)
	n.emitLocked([]FileChange{{Path: "/second/file"}}, false)
	require.Equal(t, uint64(4), (<-second).Sequence)
}

func TestChangeSocketDisconnectRemovesWideSubscription(t *testing.T) {
	v := changeGCFixture(t)
	v.changes = newChangeNotifier([]string{"/configured/**", "/skills/**"})
	v.Conf.ChangeVolume = "test"
	connect := func(pattern string) net.Conn {
		server, client := net.Pipe()
		done := make(chan struct{})
		go func() { defer close(done); defer server.Close(); v.serveChanges(server) }()
		t.Cleanup(func() { client.Close(); <-done })
		require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
		encoder, decoder := json.NewEncoder(client), json.NewDecoder(client)
		require.NoError(t, encoder.Encode(changeFrame{Type: "hello", Protocol: 1, Volume: "test"}))
		var frame changeFrame
		require.NoError(t, decoder.Decode(&frame))
		require.NoError(t, encoder.Encode(changeFrame{Type: "subscribe", ID: "subscribe", Patterns: []string{pattern}}))
		require.NoError(t, decoder.Decode(&frame))
		require.True(t, frame.OK)
		return client
	}
	narrow := connect("/skills/**")
	wide := connect("**")
	require.Eventually(t, func() bool { v.changes.Lock(); defer v.changes.Unlock(); return len(v.changes.subscribers) == 2 }, time.Second, time.Millisecond)
	require.NoError(t, wide.Close())
	require.Eventually(t, func() bool { v.changes.Lock(); defer v.changes.Unlock(); return len(v.changes.subscribers) == 1 }, time.Second, time.Millisecond)
	v.changes.Lock()
	v.changes.emitLocked([]FileChange{{Path: "/tasks/noise", Recursive: true}}, false)
	require.Zero(t, v.changes.sequence)
	require.Equal(t, []string{"/configured/**", "/skills/**"}, v.changes.patterns)
	v.changes.emitLocked([]FileChange{{Path: "/skills/demo", Recursive: true}}, false)
	v.changes.Unlock()
	var frame changeFrame
	require.NoError(t, json.NewDecoder(narrow).Decode(&frame))
	require.Len(t, frame.Changes, 1)
	require.Equal(t, "/skills/demo", frame.Changes[0].Path)
}

func TestChangeSocketSendFailureDropsSubscriptionWhileInvalidateIsBlocked(t *testing.T) {
	v := changeGCFixture(t)
	v.changes = newChangeNotifier([]string{"**"})
	v.Conf.ChangeVolume = "test"
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); defer server.Close(); v.serveChanges(server) }()
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	v.InvalidateInode = func(Ino) syscall.Errno { once.Do(func() { close(entered); <-release }); return 0 }
	t.Cleanup(func() { client.Close(); releaseOnce.Do(func() { close(release) }); <-done })
	encoder, decoder := json.NewEncoder(client), json.NewDecoder(client)
	var frame changeFrame
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, encoder.Encode(changeFrame{Type: "hello", Protocol: 1, Volume: "test"}))
	require.NoError(t, decoder.Decode(&frame))
	require.NoError(t, encoder.Encode(changeFrame{Type: "subscribe", ID: "subscribe", Patterns: []string{"**"}}))
	require.NoError(t, decoder.Decode(&frame))
	require.NoError(t, encoder.Encode(changeFrame{Type: "invalidate", ID: "blocked", Changes: []FileChange{{Path: "/", Recursive: true}}}))
	<-entered
	require.NoError(t, client.Close())
	v.changes.Lock()
	v.changes.emitLocked([]FileChange{{Path: "/unrelated", Recursive: true}}, false)
	v.changes.Unlock()
	require.Eventually(t, func() bool { v.changes.Lock(); defer v.changes.Unlock(); return len(v.changes.subscribers) == 0 }, time.Second, time.Millisecond)
	releaseOnce.Do(func() { close(release) })
}

func TestChangeFileExchangeIsNotRecursiveAndMixedExchangeIs(t *testing.T) {
	v := changeGCFixture(t)
	queue := make(chan changeFrame, 20)
	v.changes.subscribers[queue] = nil
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	first, firstHandle, st := v.Create(ctx, rootID, "first", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	second, secondHandle, st := v.Create(ctx, rootID, "second", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	t.Cleanup(func() { v.Release(ctx, first.Inode, firstHandle); v.Release(ctx, second.Inode, secondHandle) })
	<-queue
	<-queue
	require.Zero(t, v.Rename(ctx, rootID, "first", rootID, "second", meta.RenameExchange))
	frame := <-queue
	require.Len(t, frame.Changes, 1)
	require.False(t, frame.Changes[0].Recursive)
	require.Equal(t, second.Inode, v.changes.index.current[pathEdge{rootID, "first"}].inode)
	dir, st := v.Mkdir(ctx, rootID, "directory", 0755, 0)
	require.Zero(t, st)
	<-queue
	require.Zero(t, v.Rename(ctx, rootID, "first", rootID, "directory", meta.RenameExchange))
	frame = <-queue
	require.True(t, frame.Changes[0].Recursive)
	require.Equal(t, dir.Inode, v.changes.index.current[pathEdge{rootID, "first"}].inode)
}

func TestInvalidationKnownScopeDoesNotTouchUnrelatedWriter(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(rootID, "skill", 100)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	outside, handle, st := v.Create(ctx, rootID, "outside", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	t.Cleanup(func() { v.Release(ctx, outside.Inode, handle) })
	invalidated := make(map[Ino]bool)
	v.InvalidateInode = func(inode Ino) syscall.Errno { invalidated[inode] = true; return 0 }
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/skill", Recursive: true}}))
	require.True(t, invalidated[100])
	require.False(t, invalidated[outside.Inode])
}

func TestInvalidationUnknownScopeFailsBeforeTouchingCaches(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	v.changes.remember(rootID, "skill", 100)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	outside, handle, st := v.Create(ctx, rootID, "outside", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	t.Cleanup(func() { v.Release(ctx, outside.Inode, handle) })
	v.changes.remember(9001, "orphan", outside.Inode)
	var kernelCalls int
	v.InvalidateInode = func(Ino) syscall.Errno { kernelCalls++; return 0 }
	v.InvalidateEntry = func(Ino, string) syscall.Errno { kernelCalls++; return 0 }
	require.ErrorIs(t, v.InvalidatePaths([]FileChange{{Path: "/skill", Recursive: true}}), syscall.EAGAIN)
	require.Zero(t, kernelCalls)
	fields := hook.find("", "finished")
	require.Equal(t, "index_scope", fields["failure_stage"])
	require.Equal(t, "scope_uncertain", fields["result"])
	require.Equal(t, float64(9001), fields["unresolved_parent_inode"])
}

func TestInvalidationExplicitRootCoversUnpositionedCache(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.remember(9001, "orphan", 9002)
	invalidated := make(map[Ino]bool)
	v.InvalidateInode = func(inode Ino) syscall.Errno { invalidated[inode] = true; return 0 }
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/", Recursive: true}}))
	require.True(t, invalidated[9001])
	require.True(t, invalidated[9002])
}

func TestInvalidationPartialPositionCanUseProvenMatch(t *testing.T) {
	v := changeGCFixture(t)
	v.changes.index.remember(rootID, "dir", 10, 2)
	v.changes.remember(10, "file", 11)
	_, complete := v.changes.index.paths(10, true)
	require.False(t, complete)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/dir/file"}}))
}
