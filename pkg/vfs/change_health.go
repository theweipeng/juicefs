package vfs

import (
	"encoding/json"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// logChangeHealth 的诊断锁与索引锁分离，失效卡住时仍能报告未完成年龄。
func (v *VFS) logChangeHealth() {
	n := v.changes
	fields := map[string]interface{}{"event": "health_summary", "origin": n.origin, "epoch": n.epoch, "volume": v.Conf.ChangeVolume, "result": "healthy"}
	if n.TryLock() {
		fields["input_changes"], fields["filtered_changes"], fields["matched_changes"] = n.inputChanges, n.filteredChanges, n.matchedChanges
		fields["matching_total_ms"], fields["matching_max_ms"] = float64(n.matchingNanos)/1e6, float64(n.matchingMaxNanos)/1e6
		fields["index_records"], fields["index_limit"], fields["index_retired"] = n.index.size(), n.index.limit, n.index.retired.Len()
		fields["index_incomplete"], fields["position_check_required"] = n.index.incomplete, len(n.index.positionChecks) > 0
		fields["position_check_parents"] = len(n.index.positionChecks)
		patterns := make([]string, 0, 20)
		for _, pattern := range n.patterns {
			if len(patterns) == 20 {
				break
			}
			patterns = append(patterns, diagnosticText(pattern, 2048))
		}
		fields["active_subscribers"], fields["configured_patterns"], fields["configured_pattern_count"] = len(n.subscribers), patterns, len(n.patterns)
		fields["queue_overflows"], fields["sequence"] = n.overflows, n.sequence
		if n.index.incomplete {
			fields["result"] = "unavailable"
		} else if len(n.index.positionChecks) > 0 {
			fields["result"] = "degraded"
		}
		n.Unlock()
	} else {
		fields["result"], fields["index_busy"] = "degraded", true
	}
	fields["invalidation_requests"], fields["invalidation_failures"] = n.invalidations.Load(), n.invalidationFailures.Load()
	d := n.diagnostics
	d.Lock()
	lastResults := make(map[string]string)
	for source, result := range d.lastResults {
		lastResults[source] = result
		if result != "ok" && !(source == "reclaim" && result == "busy") && fields["result"] != "unavailable" {
			fields["result"] = "degraded"
		}
	}
	fields["last_results"] = lastResults
	oldest := time.Duration(0)
	for _, started := range d.active {
		if age := time.Since(started); age > oldest {
			oldest = age
		}
	}
	fields["in_flight"], fields["oldest_in_flight_ms"] = len(d.active), float64(oldest)/1e6
	if oldest > invalidationWatchAfter && fields["result"] != "unavailable" {
		fields["result"] = "degraded"
	}
	d.Unlock()
	fields["counter_scope"] = "since_mount_start"
	fields["invalidation_durations"] = changeHistogramSummary(d.durations)
	fields["invalidation_stages"] = changeHistogramSummary(d.stages)
	encoded, _ := json.Marshal(fields)
	logger.Infof("[file-change-control] %s", encoded)
}

func changeHistogramSummary(histogram *prometheus.HistogramVec) []map[string]interface{} {
	// 标签只来自三个 source 和固定阶段；有界通道避免诊断另起后台任务。
	queue := make(chan prometheus.Metric, 256)
	histogram.Collect(queue)
	close(queue)
	var result []map[string]interface{}
	for metric := range queue {
		value := &dto.Metric{}
		if metric.Write(value) != nil {
			continue
		}
		item := map[string]interface{}{"count": value.Histogram.GetSampleCount(), "total_ms": value.Histogram.GetSampleSum() * 1000}
		for _, label := range value.Label {
			item[label.GetName()] = label.GetValue()
		}
		result = append(result, item)
	}
	return result
}
