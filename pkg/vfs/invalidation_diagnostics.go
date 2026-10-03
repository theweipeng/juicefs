package vfs

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	invalidationSlowAfter   = time.Second
	invalidationWatchAfter  = 2 * time.Second
	invalidationWatchRepeat = 30 * time.Second
)

// 诊断锁独立于失效锁和路径索引锁，卡住时仍可输出阶段与当前持锁者。
type invalidationDiagnostics struct {
	sync.Mutex
	active      map[*invalidationTrace]time.Time
	owner       *invalidationTrace
	ownerSince  time.Time
	durations   *prometheus.HistogramVec
	stages      *prometheus.HistogramVec
	results     *prometheus.CounterVec
	failures    *prometheus.CounterVec
	lastResults map[string]string
}

func newInvalidationDiagnostics() *invalidationDiagnostics {
	buckets := []float64{.0001, .001, .005, .01, .05, .1, .5, 1, 2, 5, 10, 30, 60}
	return &invalidationDiagnostics{
		active:      make(map[*invalidationTrace]time.Time),
		lastResults: make(map[string]string),
		durations:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "cache_invalidation_duration_seconds", Help: "Invalidation lifetime including lock wait and control ACK delivery.", Buckets: buckets}, []string{"source"}),
		stages:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "cache_invalidation_stage_duration_seconds", Help: "Cumulative time in each invalidation stage per operation.", Buckets: buckets}, []string{"source", "stage"}),
		results:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_invalidation_results_total", Help: "Completed invalidations by bounded outcome, including index reclamation."}, []string{"source", "result"}),
		failures:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_invalidation_stage_failures_total", Help: "Failed targets by stage and bounded error code."}, []string{"source", "stage", "errno"}),
	}
}

func (d *invalidationDiagnostics) register(r prometheus.Registerer) {
	for _, metric := range []prometheus.Collector{d.durations, d.stages, d.results, d.failures} {
		_ = r.Register(metric)
	}
	for _, source := range []string{"control", "direct", "reclaim"} {
		source := source
		_ = r.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "cache_invalidation_in_flight", Help: "Requests running or waiting, including ACK delivery.", ConstLabels: prometheus.Labels{"source": source}}, func() float64 {
			d.Lock()
			defer d.Unlock()
			var count int
			for trace := range d.active {
				if trace.source == source {
					count++
				}
			}
			return float64(count)
		}))
		_ = r.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "cache_invalidation_oldest_in_flight_age_seconds", Help: "Age of the oldest unfinished operation.", ConstLabels: prometheus.Labels{"source": source}}, func() float64 {
			d.Lock()
			defer d.Unlock()
			var oldest time.Time
			for trace, started := range d.active {
				if trace.source == source && (oldest.IsZero() || started.Before(oldest)) {
					oldest = started
				}
			}
			if oldest.IsZero() {
				return 0
			}
			return time.Since(oldest).Seconds()
		}))
	}
	_ = r.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "cache_invalidation_lock_held_seconds", Help: "Current continuous invalidation lock hold time; zero when idle."}, func() float64 {
		d.Lock()
		defer d.Unlock()
		if d.owner == nil {
			return 0
		}
		return time.Since(d.ownerSince).Seconds()
	}))
}

type invalidationTrace struct {
	sync.Mutex
	d              *invalidationDiagnostics
	source         string
	started        time.Time
	phaseStarted   time.Time
	stage          string
	fields         map[string]interface{}
	stageDurations map[string]time.Duration
	failureErr     error
	failureStage   string
	failureCount   int
	failureSamples []map[string]interface{}
	done           chan struct{}
	watchDone      chan struct{}
}

func diagnosticText(value string, limit int) string {
	value = strings.ToValidUTF8(value, "\ufffd")
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func diagnosticRequestID(id string) string {
	if id == "" {
		return uuid.NewString()
	}
	if len(id) <= 128 && utf8.ValidString(id) {
		return id
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(id)))
}

func (v *VFS) newInvalidationTrace(source, id, connection string, changes []FileChange) *invalidationTrace {
	now := time.Now()
	t := &invalidationTrace{source: source, started: now, phaseStarted: now, stage: "validate", fields: make(map[string]interface{}), stageDurations: make(map[string]time.Duration), done: make(chan struct{}), watchDone: make(chan struct{})}
	if v.changes != nil {
		t.d = v.changes.diagnostics
	}
	t.fields["request_id"], t.fields["connection_id"] = diagnosticRequestID(id), connection
	t.fields["source"] = source
	if v.Conf != nil {
		t.fields["volume"] = diagnosticText(v.Conf.ChangeVolume, 2048)
		t.fields["socket"] = diagnosticText(v.Conf.ChangeSocket, 2048)
		t.fields["version"] = diagnosticText(v.Conf.Version, 256)
		if v.Conf.Meta != nil {
			t.fields["mount_point"] = diagnosticText(v.Conf.Meta.MountPoint, 2048)
		}
	}
	if v.changes != nil {
		t.fields["origin"], t.fields["epoch"] = v.changes.origin, v.changes.epoch
	}
	ordered := append([]FileChange(nil), changes...)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		if a.OldPath != b.OldPath {
			return a.OldPath < b.OldPath
		}
		if a.Recursive != b.Recursive {
			return !a.Recursive
		}
		return a.Operation < b.Operation
	})
	encoded, _ := json.Marshal(ordered)
	t.fields["request_digest"] = fmt.Sprintf("%x", sha256.Sum256(encoded))
	paths := make([]string, 0, 20)
	count, recursive, truncated := 0, 0, 0
	for _, change := range ordered {
		if change.Recursive {
			recursive++
		}
		for _, value := range []string{change.Path, change.OldPath} {
			if value == "" {
				continue
			}
			count++
			if len(paths) < 20 {
				if len(value) > 2048 {
					truncated++
				}
				paths = append(paths, diagnosticText(value, 2048))
			}
		}
	}
	t.fields["paths"], t.fields["path_count"], t.fields["recursive_count"] = paths, count, recursive
	t.fields["omitted_paths"], t.fields["truncated_paths"] = count-len(paths), truncated
	if t.d != nil {
		t.d.Lock()
		t.d.active[t] = now
		t.d.Unlock()
	}
	go t.watch()
	return t
}

func (t *invalidationTrace) phase(stage string, inode Ino, edge pathEdge) {
	if t == nil {
		return
	}
	t.Lock()
	now := time.Now()
	t.stageDurations[t.stage] += now.Sub(t.phaseStarted)
	t.stage, t.phaseStarted = stage, now
	t.fields["inode"], t.fields["parent_inode"], t.fields["entry_name"] = inode, edge.parent, diagnosticText(edge.name, 256)
	delete(t.fields, "fh")
	t.Unlock()
}

func (t *invalidationTrace) detail(fields map[string]interface{}) {
	if t == nil {
		return
	}
	t.Lock()
	for key, value := range fields {
		t.fields[key] = value
	}
	t.Unlock()
}

func (t *invalidationTrace) lockOwner(acquired bool) {
	if t == nil || t.d == nil {
		return
	}
	t.d.Lock()
	if acquired {
		t.d.owner, t.d.ownerSince = t, time.Now()
	} else if t.d.owner == t {
		t.d.owner = nil
	}
	t.d.Unlock()
}

func invalidationErrno(err error) syscall.Errno {
	var number syscall.Errno
	if errors.As(err, &number) {
		return number
	}
	return 0
}

func (t *invalidationTrace) failure(err error) {
	if t == nil || err == nil {
		return
	}
	t.Lock()
	defer t.Unlock()
	// 回收中正常的活跃句柄不能掩盖同一批次已出现的内核错误。
	if !(t.source == "reclaim" && invalidationResult(err, t.stage) == "busy" && t.failureErr != nil && invalidationResult(t.failureErr, t.failureStage) != "busy") {
		t.failureErr, t.failureStage = err, t.stage
	}
	t.failureCount++
	if t.d != nil {
		code := "OTHER"
		switch number := invalidationErrno(err); number {
		case syscall.EBUSY, syscall.ENOBUFS, syscall.ENOTSUP, syscall.EINVAL, syscall.E2BIG, syscall.EIO, syscall.EPERM, syscall.EINTR, syscall.ENOENT, syscall.ENOSYS, syscall.EAGAIN:
			code = utils.ErrnoName(number)
		}
		t.d.failures.WithLabelValues(t.source, t.stage, code).Inc()
	}
	if len(t.failureSamples) < 8 {
		t.failureSamples = append(t.failureSamples, map[string]interface{}{"stage": t.stage, "inode": t.fields["inode"], "parent_inode": t.fields["parent_inode"], "entry_name": t.fields["entry_name"], "errno": int(invalidationErrno(err))})
	}
}

func (t *invalidationTrace) busy(v *VFS, inode Ino, h *handle, reason string) {
	if t == nil {
		return
	}
	fields := map[string]interface{}{"busy_inode": inode, "busy_fh": h.fh, "busy_reason": reason}
	// 日志补充不能让本应立即拒绝的请求再等待业务锁。
	if h.TryLock() {
		fields["busy_flags"], fields["busy_opener_pid"], fields["busy_opener_uid"] = h.flags, h.openPID, h.openUID
		if !h.openedAt.IsZero() {
			fields["busy_open_age_ms"] = time.Since(h.openedAt).Seconds() * 1000
		}
		h.Unlock()
	} else {
		fields["busy_handle_metadata_unavailable"] = true
	}
	t.detail(fields)
	// 只解析失败 inode，不读取共享目录；该阶段自身也受慢请求监控。
	t.phase("busy_path_lookup", inode, pathEdge{})
	var paths []string
	var complete bool
	if v.changes.TryLock() {
		paths, complete = v.changes.index.paths(inode, true)
		v.changes.Unlock()
	} else {
		t.detail(map[string]interface{}{"busy_paths_unavailable": true})
	}
	count := len(paths)
	if len(paths) > 20 {
		paths = paths[:20]
	}
	for i := range paths {
		paths[i] = diagnosticText(paths[i], 2048)
	}
	t.detail(map[string]interface{}{"busy_paths": paths, "busy_paths_complete": complete, "busy_paths_omitted": count - len(paths)})
	t.phase("busy_handles", inode, pathEdge{})
}

func (t *invalidationTrace) snapshot() map[string]interface{} {
	t.Lock()
	defer t.Unlock()
	fields := make(map[string]interface{}, len(t.fields)+10)
	for key, value := range t.fields {
		fields[key] = value
	}
	fields["stage"], fields["duration_ms"], fields["stage_elapsed_ms"] = t.stage, time.Since(t.started).Seconds()*1000, time.Since(t.phaseStarted).Seconds()*1000
	durations := make(map[string]float64, len(t.stageDurations)+1)
	for stage, duration := range t.stageDurations {
		durations[stage] = duration.Seconds() * 1000
	}
	durations[t.stage] += time.Since(t.phaseStarted).Seconds() * 1000
	fields["stage_durations_ms"] = durations
	if t.failureErr != nil {
		fields["failure_stage"], fields["errno"] = t.failureStage, int(invalidationErrno(t.failureErr))
		fields["error_code"], fields["failure_count"] = utils.ErrnoName(invalidationErrno(t.failureErr)), t.failureCount
		fields["failure_samples"] = append([]map[string]interface{}(nil), t.failureSamples...)
	}
	return fields
}

func (t *invalidationTrace) emit(event string, level logrus.Level) {
	if !logger.IsLevelEnabled(level) {
		return
	}
	fields := t.snapshot()
	fields["event"] = event
	if t.d != nil {
		t.d.Lock()
		owner := t.d.owner
		ownerSince := t.d.ownerSince
		t.d.Unlock()
		if owner != nil && owner != t {
			other := owner.snapshot()
			fields["lock_owner"] = map[string]interface{}{"request_id": other["request_id"], "source": other["source"], "stage": other["stage"], "duration_ms": other["duration_ms"], "lock_held_ms": time.Since(ownerSince).Seconds() * 1000, "paths": other["paths"], "inode": other["inode"], "parent_inode": other["parent_inode"], "entry_name": other["entry_name"]}
		}
	}
	encoded, _ := json.Marshal(fields)
	// 外层 Log 是云 SDK 的 DEBUG 适配器；显式调用底层，保证异常不被降级。
	logger.Logger.Log(level, "[cache-invalidation] ", string(encoded))
}

func (t *invalidationTrace) watch() {
	defer close(t.watchDone)
	timer := time.NewTimer(invalidationWatchAfter)
	defer timer.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-timer.C:
			t.emit("blocked", logrus.WarnLevel)
			timer.Reset(invalidationWatchRepeat)
		}
	}
}

func invalidationResult(err error, stage string) string {
	if err == nil {
		return "ok"
	}
	if stage == "ack_send" {
		return "ack_error"
	}
	if stage == "index_scope" {
		return "scope_uncertain"
	}
	if stage == "kernel_inode" || stage == "kernel_entry" {
		return "kernel_error"
	}
	switch invalidationErrno(err) {
	case syscall.EBUSY:
		return "busy"
	case syscall.ENOBUFS:
		return "capacity"
	case syscall.ENOTSUP:
		return "unsupported"
	case syscall.E2BIG, syscall.EINVAL:
		return "invalid"
	}
	if stage == "validate" {
		return "invalid"
	}
	return "other"
}

func (t *invalidationTrace) finish(err error) {
	if t == nil {
		return
	}
	if err != nil {
		t.Lock()
		recorded := t.failureErr != nil
		t.Unlock()
		if !recorded {
			t.failure(err)
		}
	}
	close(t.done)
	<-t.watchDone
	t.Lock()
	t.stageDurations[t.stage] += time.Since(t.phaseStarted)
	t.phaseStarted = time.Now()
	result := invalidationResult(t.failureErr, t.failureStage)
	t.fields["result"] = result
	if t.d != nil {
		t.d.durations.WithLabelValues(t.source).Observe(time.Since(t.started).Seconds())
		for stage, duration := range t.stageDurations {
			t.d.stages.WithLabelValues(t.source, stage).Observe(duration.Seconds())
		}
		t.d.results.WithLabelValues(t.source, result).Inc()
	}
	t.Unlock()
	if t.d != nil {
		t.d.Lock()
		delete(t.d.active, t)
		t.d.lastResults[t.source] = result
		t.d.Unlock()
	}
	level := logrus.DebugLevel
	if time.Since(t.started) >= invalidationSlowAfter || (result != "ok" && !(t.source == "reclaim" && result == "busy")) {
		level = logrus.WarnLevel
	}
	t.emit("finished", level)
}
