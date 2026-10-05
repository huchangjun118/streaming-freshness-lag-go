package monitor

import (
	"sort"
	"time"
)

// partitionState 单个分区的跟踪状态。进度只取历史最大，水位单调不回退。
type partitionState struct {
	WriteOffset   int64        `json:"write_offset"`
	ConsumeOffset int64        `json:"consume_offset"`
	Watermark     time.Time    `json:"watermark"`
	LastSeq       int64        `json:"last_seq"`
	Regressions   []Regression `json:"regressions,omitempty"`
}

// tracker 管理所有分区状态，处理乱序/重复/回退。
// 核心不变量：offset 取 max，watermark 取 max，因此重复与乱序上报
// 不会改变按真实进度得到的判定结果。
type tracker struct {
	maxRegressions int
	parts          map[string]*partitionState
}

func newTracker(maxRegressions int) *tracker {
	if maxRegressions <= 0 {
		maxRegressions = 64
	}
	return &tracker{maxRegressions: maxRegressions, parts: make(map[string]*partitionState)}
}

// apply 应用一条上报。返回分区状态与是否被接受（false 表示重复或回退，
// 不影响已确定的真实进度）。
func (t *tracker) apply(r Report, now time.Time) (*partitionState, bool) {
	ps, ok := t.parts[r.Partition]
	if !ok {
		ps = &partitionState{}
		t.parts[r.Partition] = ps
	}
	// 序列号去重：重启恢复后已提交过的上报直接跳过。
	if r.Seq != 0 && r.Seq <= ps.LastSeq {
		return ps, false
	}
	if r.Seq > ps.LastSeq {
		ps.LastSeq = r.Seq
	}

	cur := ps.WriteOffset
	if r.Kind == KindConsume {
		cur = ps.ConsumeOffset
	}
	switch {
	case r.Offset > cur:
		if r.Kind == KindWrite {
			ps.WriteOffset = r.Offset
		} else {
			ps.ConsumeOffset = r.Offset
		}
	case r.Offset == cur:
		// 重复上报，幂等。
	default:
		// 进度回退：记录异常，但不接受回退值，保证结果不随时间倒流。
		t.recordRegression(ps, Regression{
			Partition: r.Partition,
			Kind:      r.Kind,
			Prev:      cur,
			Got:       r.Offset,
			At:        now,
		})
		return ps, false
	}

	// 水位只前进不后退：迟到数据的事件时间早于水位时不影响水位。
	if r.Kind == KindWrite && r.EventTime.After(ps.Watermark) {
		ps.Watermark = r.EventTime
	}
	return ps, true
}

func (t *tracker) recordRegression(ps *partitionState, reg Regression) {
	ps.Regressions = append(ps.Regressions, reg)
	if len(ps.Regressions) > t.maxRegressions {
		// 有界：丢弃最旧记录，内存不随运行时间增长。
		ps.Regressions = append([]Regression(nil), ps.Regressions[len(ps.Regressions)-t.maxRegressions:]...)
	}
}

// metrics 由分区状态计算指标快照。
func (t *tracker) metrics(partition string, now time.Time) (PartitionMetrics, bool) {
	ps, ok := t.parts[partition]
	if !ok {
		return PartitionMetrics{}, false
	}
	return metricsOf(partition, ps, now), true
}

func metricsOf(partition string, ps *partitionState, now time.Time) PartitionMetrics {
	lag := ps.WriteOffset - ps.ConsumeOffset
	if lag < 0 {
		lag = 0
	}
	var freshness time.Duration
	if !ps.Watermark.IsZero() && now.After(ps.Watermark) {
		freshness = now.Sub(ps.Watermark)
	}
	return PartitionMetrics{
		Partition:     partition,
		WriteOffset:   ps.WriteOffset,
		ConsumeOffset: ps.ConsumeOffset,
		Lag:           lag,
		Freshness:     freshness,
		Watermark:     ps.Watermark,
		UpdatedAt:     now,
	}
}

// snapshot 返回所有分区指标，按分区名排序保证输出稳定。
func (t *tracker) snapshot(now time.Time) []PartitionMetrics {
	names := t.partitions()
	out := make([]PartitionMetrics, 0, len(names))
	for _, n := range names {
		out = append(out, metricsOf(n, t.parts[n], now))
	}
	return out
}

// regressions 返回某分区的回退记录副本。
func (t *tracker) regressions(partition string) []Regression {
	ps, ok := t.parts[partition]
	if !ok {
		return nil
	}
	return append([]Regression(nil), ps.Regressions...)
}

// partitions 返回当前分区列表（排序）。
func (t *tracker) partitions() []string {
	names := make([]string, 0, len(t.parts))
	for n := range t.parts {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// loadState 从 checkpoint 恢复分区状态。
func (t *tracker) loadState(parts map[string]partitionState) {
	for name, ps := range parts {
		cp := ps
		t.parts[name] = &cp
	}
}

// dumpState 导出全部分区状态用于 checkpoint。
func (t *tracker) dumpState() map[string]partitionState {
	out := make(map[string]partitionState, len(t.parts))
	for name, ps := range t.parts {
		out[name] = *ps
	}
	return out
}
