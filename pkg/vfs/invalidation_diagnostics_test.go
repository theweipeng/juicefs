//go:build !windows

package vfs

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type invalidationLogHook struct {
	sync.Mutex
	records []map[string]interface{}
	levels  []logrus.Level
}

func (h *invalidationLogHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *invalidationLogHook) Fire(entry *logrus.Entry) error {
	const prefix = "[cache-invalidation] "
	if !strings.HasPrefix(entry.Message, prefix) {
		return nil
	}
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(entry.Message, prefix)), &fields); err != nil {
		return err
	}
	h.Lock()
	h.records, h.levels = append(h.records, fields), append(h.levels, entry.Level)
	h.Unlock()
	return nil
}

func captureInvalidationLogs(t *testing.T) *invalidationLogHook {
	t.Helper()
	hook := &invalidationLogHook{}
	old := logger.ReplaceHooks(make(logrus.LevelHooks))
	logger.AddHook(hook)
	t.Cleanup(func() { logger.ReplaceHooks(old) })
	return hook
}

func (h *invalidationLogHook) find(id, event string) map[string]interface{} {
	h.Lock()
	defer h.Unlock()
	for _, fields := range h.records {
		if (id == "" || fields["request_id"] == id) && fields["event"] == event {
			return fields
		}
	}
	return nil
}

func assertInvalidationMetric(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, expected float64) {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			got := make(map[string]string)
			for _, label := range metric.Label {
				got[label.GetName()] = label.GetValue()
			}
			if fmt.Sprint(got) != fmt.Sprint(labels) {
				continue
			}
			if metric.Counter != nil {
				require.Equal(t, expected, metric.Counter.GetValue())
			} else {
				require.Equal(t, expected, metric.Gauge.GetValue())
			}
			return
		}
	}
	t.Fatalf("metric missing: %s %v", name, labels)
}

func TestInvalidationDiagnosticsBusyIdentifiesFileAndOpener(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	ctx := NewLogContext(meta.NewContext(4242, 123, []uint32{123}))
	entry, fh, st := v.Create(ctx, rootID, "busy-skill", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	t.Cleanup(func() { v.Release(ctx, entry.Inode, fh) })
	trace := v.newInvalidationTrace("control", "busy-request", "connection-1", []FileChange{{Path: "/busy-skill"}})
	err := v.invalidatePaths([]FileChange{{Path: "/busy-skill"}}, trace)
	trace.finish(err)
	require.ErrorIs(t, err, syscall.EBUSY)
	fields := hook.find("busy-request", "finished")
	require.NotNil(t, fields)
	require.Equal(t, "busy_handles", fields["failure_stage"])
	require.Equal(t, "busy", fields["result"])
	require.Equal(t, float64(syscall.EBUSY), fields["errno"])
	require.Equal(t, float64(entry.Inode), fields["busy_inode"])
	require.Equal(t, float64(fh), fields["busy_fh"])
	require.Equal(t, float64(4242), fields["busy_opener_pid"])
	require.Equal(t, float64(123), fields["busy_opener_uid"])
	require.Equal(t, []interface{}{"/busy-skill"}, fields["busy_paths"])
	require.Equal(t, true, fields["busy_paths_complete"])
	require.Equal(t, "writable_handle", fields["busy_reason"])
	require.GreaterOrEqual(t, fields["busy_open_age_ms"].(float64), float64(0))
	registry := prometheus.NewRegistry()
	v.changes.diagnostics.register(registry)
	assertInvalidationMetric(t, registry, "cache_invalidation_results_total", map[string]string{"source": "control", "result": "busy"}, 1)
	assertInvalidationMetric(t, registry, "cache_invalidation_stage_failures_total", map[string]string{"source": "control", "stage": "busy_handles", "errno": "EBUSY"}, 1)
}

func TestInvalidationDiagnosticsBusyEnrichmentDoesNotWaitOnLocks(t *testing.T) {
	v := changeGCFixture(t)
	h := v.newHandle(10, false)
	trace := v.newInvalidationTrace("direct", "enrichment", "", nil)
	h.Lock()
	v.changes.Lock()
	done := make(chan struct{})
	go func() { trace.busy(v, 10, h, "writable_handle"); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		h.Unlock()
		v.changes.Unlock()
		<-done
		trace.finish(nil)
		t.Fatal("diagnostic enrichment waited for a filesystem lock")
	}
	h.Unlock()
	v.changes.Unlock()
	fields := trace.snapshot()
	require.Equal(t, true, fields["busy_handle_metadata_unavailable"])
	require.Equal(t, true, fields["busy_paths_unavailable"])
	require.Equal(t, false, fields["busy_paths_complete"])
	trace.finish(nil)
	v.releaseHandle(10, h.fh)
}

func TestInvalidationDiagnosticsBlockedKernelAndWaitingOwner(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	v.changes.remember(rootID, "skill", 10)
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	v.InvalidateInode = func(ino Ino) syscall.Errno {
		if ino == 10 {
			enterOnce.Do(func() { close(entered); <-release })
		}
		return 0
	}
	first := v.newInvalidationTrace("control", "kernel-owner", "connection-a", []FileChange{{Path: "/skill"}})
	doneA := make(chan error, 1)
	go func() {
		err := v.invalidatePaths([]FileChange{{Path: "/skill"}}, first)
		first.finish(err)
		doneA <- err
	}()
	defer releaseOnce.Do(func() { close(release) })
	<-entered
	second := v.newInvalidationTrace("control", "lock-waiter", "connection-b", []FileChange{{Path: "/other"}})
	doneB := make(chan error, 1)
	go func() {
		err := v.invalidatePaths([]FileChange{{Path: "/other"}}, second)
		second.finish(err)
		doneB <- err
	}()
	require.Eventually(t, func() bool {
		return hook.find("kernel-owner", "blocked") != nil && hook.find("lock-waiter", "blocked") != nil
	}, 4*time.Second, 10*time.Millisecond)
	blocked := hook.find("kernel-owner", "blocked")
	require.Equal(t, "kernel_inode", blocked["stage"])
	require.Equal(t, float64(10), blocked["inode"])
	waiting := hook.find("lock-waiter", "blocked")
	require.Equal(t, "invalidation_lock", waiting["stage"])
	owner := waiting["lock_owner"].(map[string]interface{})
	require.Equal(t, "kernel-owner", owner["request_id"])
	require.Equal(t, "kernel_inode", owner["stage"])
	require.Equal(t, []interface{}{"/skill"}, owner["paths"])
	registry := prometheus.NewRegistry()
	v.changes.diagnostics.register(registry)
	assertInvalidationMetric(t, registry, "cache_invalidation_in_flight", map[string]string{"source": "control"}, 2)
	require.Nil(t, hook.find("kernel-owner", "finished"))
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-doneA)
	require.NoError(t, <-doneB)
	assertInvalidationMetric(t, registry, "cache_invalidation_in_flight", map[string]string{"source": "control"}, 0)
	assertInvalidationMetric(t, registry, "cache_invalidation_lock_held_seconds", nil, 0)
	finished := hook.find("lock-waiter", "finished")
	require.Equal(t, "ok", finished["result"])
	require.GreaterOrEqual(t, finished["stage_durations_ms"].(map[string]interface{})["invalidation_lock"].(float64), float64(1900))
}

func TestInvalidationDiagnosticsWatchDoesNotNeedIndexLock(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	v.changes.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(v.changes.Unlock) }
	defer release()
	trace := v.newInvalidationTrace("control", "index-lock", "connection", []FileChange{{Path: "/skill"}})
	done := make(chan error, 1)
	go func() {
		err := v.invalidatePaths([]FileChange{{Path: "/skill"}}, trace)
		trace.finish(err)
		done <- err
	}()
	require.Eventually(t, func() bool { return hook.find("index-lock", "blocked") != nil }, 4*time.Second, 10*time.Millisecond)
	require.Equal(t, "index_lock", hook.find("index-lock", "blocked")["stage"])
	release()
	require.NoError(t, <-done)
}

func TestInvalidationDiagnosticsKernelFailuresAreNotBusyProtection(t *testing.T) {
	for _, stage := range []string{"kernel_inode", "kernel_entry"} {
		t.Run(stage, func(t *testing.T) {
			hook := captureInvalidationLogs(t)
			v := changeGCFixture(t)
			v.changes.remember(rootID, "skill", 10)
			if stage == "kernel_inode" {
				v.InvalidateInode = func(Ino) syscall.Errno { return syscall.EBUSY }
			} else {
				v.InvalidateEntry = func(Ino, string) syscall.Errno { return syscall.EIO }
			}
			require.Error(t, v.InvalidatePaths([]FileChange{{Path: "/skill"}}))
			fields := hook.find("", "finished")
			require.Equal(t, stage, fields["failure_stage"])
			require.Equal(t, "kernel_error", fields["result"])
			require.NotEmpty(t, fields["failure_samples"])
			require.NotContains(t, fields, "busy_fh")
		})
	}
}

func TestInvalidationDiagnosticsQuietSuccessAndBoundedLabels(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	v.changes.remember(rootID, "skill", 10)
	require.NoError(t, v.InvalidatePaths([]FileChange{{Path: "/skill"}}))
	require.Nil(t, hook.find("", "finished"))
	registry := prometheus.NewRegistry()
	initVFSMetrics(v, v.writer, v.reader, registry)
	assertInvalidationMetric(t, registry, "cache_invalidation_results_total", map[string]string{"source": "direct", "result": "ok"}, 1)
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if !strings.HasPrefix(family.GetName(), "cache_invalidation_") {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				require.Contains(t, []string{"source", "stage", "result", "errno"}, label.GetName())
			}
		}
	}
}

func TestInvalidationDiagnosticsBoundsAndEscapesUntrustedPaths(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	var changes []FileChange
	for i := 0; i < 24; i++ {
		changes = append(changes, FileChange{Path: fmt.Sprintf("/%02d\n%s", i, strings.Repeat("路径", 1024)), OldPath: "/old", Recursive: true})
	}
	trace := v.newInvalidationTrace("control", strings.Repeat("id", 3000), "connection", changes)
	trace.finish(syscall.EIO)
	fields := hook.find("", "finished")
	require.Len(t, fields["paths"], 20)
	require.Equal(t, float64(48), fields["path_count"])
	require.Equal(t, float64(28), fields["omitted_paths"])
	require.Len(t, fields["request_digest"], 64)
	require.True(t, strings.HasPrefix(fields["request_id"].(string), "sha256:"))
	for _, value := range fields["paths"].([]interface{}) {
		require.LessOrEqual(t, len(value.(string)), 2048)
		require.True(t, utf8.ValidString(value.(string)))
	}
	encoded, err := json.Marshal(fields)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "\n")
	trace2 := v.newInvalidationTrace("control", "reordered", "connection", []FileChange{changes[1], changes[0]})
	trace3 := v.newInvalidationTrace("control", "ordered", "connection", changes[:2])
	require.Equal(t, trace2.snapshot()["request_digest"], trace3.snapshot()["request_digest"])
	trace2.finish(nil)
	trace3.finish(nil)
}

func TestInvalidationDiagnosticsReclaimErrorCannotBeHiddenByBusy(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	v.changes.remember(rootID, "old", 10)
	v.changes.remember(rootID, "old", 11)
	ctx := NewLogContext(meta.NewContext(42, 0, []uint32{0}))
	entry, fh, st := v.Create(ctx, rootID, "busy", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	t.Cleanup(func() { v.Release(ctx, entry.Inode, fh) })
	v.changes.remember(rootID, "busy", entry.Inode+100)
	v.InvalidateInode = func(ino Ino) syscall.Errno {
		if ino == 10 {
			return syscall.EIO
		}
		return 0
	}
	v.collectChangeIndex(time.Now())
	fields := hook.find("", "finished")
	require.NotNil(t, fields)
	require.Equal(t, "reclaim", fields["source"])
	require.Equal(t, "kernel_error", fields["result"])
	require.Equal(t, "kernel_inode", fields["failure_stage"])
	require.GreaterOrEqual(t, fields["failure_count"].(float64), float64(2))
}

func TestChangeSocketDiagnosticsACKAndBlockedDelivery(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	v.Conf.ChangeVolume = "test"
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); defer server.Close(); v.serveChanges(server) }()
	t.Cleanup(func() { client.Close(); <-done })
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	encoder, decoder := json.NewEncoder(client), json.NewDecoder(client)
	require.NoError(t, encoder.Encode(changeFrame{Type: "hello", Protocol: 1, Volume: "test"}))
	var response changeFrame
	require.NoError(t, decoder.Decode(&response))
	require.NoError(t, encoder.Encode(changeFrame{Type: "invalidate", ID: "bad-path", Changes: []FileChange{{Path: "/../bad"}}}))
	require.NoError(t, decoder.Decode(&response))
	require.False(t, response.OK)
	require.Equal(t, "bad-path", response.ID)
	require.Equal(t, int(syscall.EINVAL), response.Errno)
	require.Equal(t, "validate", response.FailureStage)
	require.NoError(t, encoder.Encode(changeFrame{Type: "invalidate", ID: "blocked-ack", Changes: []FileChange{{Path: "/new"}}}))
	// 不消费响应，验证发送阶段也能在完成前告警。
	require.Eventually(t, func() bool { return hook.find("blocked-ack", "blocked") != nil }, 4*time.Second, 10*time.Millisecond)
	fields := hook.find("blocked-ack", "blocked")
	require.Equal(t, "ack_send", fields["stage"])
	require.NotEmpty(t, fields["connection_id"])
	require.NoError(t, decoder.Decode(&response))
	require.True(t, response.OK)
	require.Equal(t, "blocked-ack", response.ID)
	// 三方都能使用原始请求 ID，不另造无法关联的挂载端 ID。
	require.Eventually(t, func() bool { return hook.find("blocked-ack", "finished") != nil }, time.Second, time.Millisecond)
	require.Equal(t, "ok", hook.find("blocked-ack", "finished")["result"])
}

func TestChangeSocketDiagnosticsACKDisconnect(t *testing.T) {
	hook := captureInvalidationLogs(t)
	v := changeGCFixture(t)
	v.Conf.ChangeVolume = "test"
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); defer server.Close(); v.serveChanges(server) }()
	t.Cleanup(func() { client.Close(); <-done })
	encoder, decoder := json.NewEncoder(client), json.NewDecoder(client)
	require.NoError(t, encoder.Encode(changeFrame{Type: "hello", Protocol: 1, Volume: "test"}))
	var response changeFrame
	require.NoError(t, decoder.Decode(&response))
	require.NoError(t, encoder.Encode(changeFrame{Type: "invalidate", ID: "closed-ack", Changes: []FileChange{{Path: "/new"}}}))
	require.NoError(t, client.Close())
	<-done
	fields := hook.find("closed-ack", "finished")
	require.NotNil(t, fields)
	require.Equal(t, "ack_send", fields["failure_stage"])
	require.Equal(t, "ack_error", fields["result"])
}

func BenchmarkInvalidationDiagnostics(b *testing.B) {
	for _, size := range []int{0, 100, 10000} {
		b.Run(fmt.Sprintf("index-%d", size), func(b *testing.B) {
			v, _ := createTestVFS(nil, "")
			v.changes = newChangeNotifier(nil)
			v.InvalidateInode = func(Ino) syscall.Errno { return 0 }
			v.InvalidateEntry = func(Ino, string) syscall.Errno { return 0 }
			for i := 0; i < size; i++ {
				v.changes.remember(rootID, fmt.Sprintf("file-%d", i), Ino(i+10))
			}
			changes := []FileChange{{Path: "/file-0"}}
			for _, diagnostics := range []bool{false, true} {
				b.Run(fmt.Sprintf("diagnostics-%t", diagnostics), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var err error
						if diagnostics {
							err = v.InvalidatePaths(changes)
						} else {
							err = v.invalidatePaths(changes, nil)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
