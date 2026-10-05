package monitor

import (
	"fmt"
	"time"
)

// dedupWindow 是滑动位图去重窗口：对同一来源的 Seq，
// 区分 新上报 / 迟到上报 / 重复上报 / 过期上报 四种情况。
type dedupWindow struct {
	size uint64
	max  uint64
	init bool
	bits []uint64
}

func newDedupWindow(size int) *dedupWindow {
	if size < 64 {
		size = 64
	}
	return &dedupWindow{size: uint64(size), bits: make([]uint64, (size+63)/64)}
}

type dedupVerdict int

const (
	dedupNew       dedupVerdict = iota // 序号前进，最新上报
	dedupLate                          // 窗口内的乱序迟到上报
	dedupDuplicate                     // 重复上报
	dedupExpired                       // 序号已滑出窗口，无法判定
)

func (d *dedupWindow) classify(seq uint64) dedupVerdict {
	if !d.init {
		d.init = true
		d.max = seq
		d.set(seq)
		return dedupNew
	}
	if seq > d.max {
		// 前进：清除 (max, seq] 之间的旧位再置位。
		if seq-d.max >= d.size {
			for i := range d.bits {
				d.bits[i] = 0
			}
		} else {
			for i := d.max + 1; i <= seq; i++ {
				d.clear(i)
			}
		}
		d.max = seq
		d.set(seq)
		return dedupNew
	}
	if seq+d.size <= d.max {
		return dedupExpired
	}
	if d.test(seq) {
		return dedupDuplicate
	}
	d.set(seq)
	return dedupLate
}

func (d *dedupWindow) pos(seq uint64) (int, uint64) {
	p := seq % d.size
	return int(p / 64), uint64(1) << (p % 64)
}

func (d *dedupWindow) set(seq uint64)   { i, m := d.pos(seq); d.bits[i] |= m }
func (d *dedupWindow) clear(seq uint64) { i, m := d.pos(seq); d.bits[i] &^= m }
func (d *dedupWindow) test(seq uint64) bool {
	i, m := d.pos(seq)
	return d.bits[i]&m != 0
}

// streamState 一路（写或消费）进度的状态。
type streamState struct {
	offset    int64
	watermark time.Time
	init      bool
	sources   map[string]*dedupWindow
	window    int
}

func newStreamState(window int) streamState {
	return streamState{sources: make(map[string]*dedupWindow), window: window}
}

// applyResult 描述一次上报的应用结果。
type applyResult struct {
	changed     bool
	regressions []Regression
	drop        *DropRecord
}

// apply 将一条上报合并进单路状态。
//
// 策略：
//   - 重复上报：直接忽略，状态不变。
//   - 过期上报（滑出去重窗口）：丢弃并记录，状态不变。
//   - 乱序迟到上报：按 max 语义合并，不视为回退。
//   - 序号前进但进度值回退：识别为回退，记录并保持状态不回退。
func (s *streamState) apply(p Progress, now time.Time) applyResult {
	src := p.Source
	if src == "" {
		src = "default"
	}
	w, ok := s.sources[src]
	if !ok {
		w = newDedupWindow(s.window)
		s.sources[src] = w
	}
	switch w.classify(p.Seq) {
	case dedupDuplicate:
		return applyResult{}
	case dedupExpired:
		return applyResult{drop: &DropRecord{
			Partition: p.Partition,
			Reason:    DropReasonDedupWindow,
			Detail:    fmt.Sprintf("kind=%s source=%s seq=%d 已滑出去重窗口", p.Kind, src, p.Seq),
			At:        now,
		}}
	}
	// dedupNew 或 dedupLate：按 max 语义合并。
	res := applyResult{}
	if !s.init {
		s.init = true
		s.offset = p.Offset
		s.watermark = p.EventTime
		res.changed = true
		return res
	}
	if p.Offset > s.offset {
		s.offset = p.Offset
		res.changed = true
	} else if p.Offset < s.offset {
		if w, _ := s.sources[src]; w != nil && p.Seq >= w.max {
			// 序号未回退而进度回退：真实回退异常。
			res.regressions = append(res.regressions, Regression{
				Partition: p.Partition,
				Kind:      p.Kind,
				Field:     "offset",
				Previous:  fmt.Sprintf("%d", s.offset),
				Current:   fmt.Sprintf("%d", p.Offset),
				At:        now,
			})
		}
		// 状态不回退。
	}
	if p.EventTime.After(s.watermark) {
		s.watermark = p.EventTime
		res.changed = true
	} else if p.EventTime.Before(s.watermark) {
		if w, _ := s.sources[src]; w != nil && p.Seq >= w.max {
			res.regressions = append(res.regressions, Regression{
				Partition: p.Partition,
				Kind:      p.Kind,
				Field:     "event_time",
				Previous:  s.watermark.UTC().Format(time.RFC3339Nano),
				Current:   p.EventTime.UTC().Format(time.RFC3339Nano),
				At:        now,
			})
		}
	}
	return res
}

// tracker 维护单个分区的写/消费两路进度状态。
type tracker struct {
	partition   PartitionID
	write       streamState
	consume     streamState
	regressions uint64
	dropped     uint64
}

func newTracker(p PartitionID, dedupWindowSize int) *tracker {
	return &tracker{
		partition: p,
		write:     newStreamState(dedupWindowSize),
		consume:   newStreamState(dedupWindowSize),
	}
}

// apply 应用一条进度上报。
func (t *tracker) apply(p Progress, now time.Time) applyResult {
	var res applyResult
	switch p.Kind {
	case WriteProgress:
		res = t.write.apply(p, now)
	case ConsumeProgress:
		res = t.consume.apply(p, now)
	}
	t.regressions += uint64(len(res.regressions))
	if res.drop != nil {
		t.dropped++
	}
	return res
}

// metrics 生成当前快照。新鲜度与积压延迟只前进不回退：
// 水位与位移在 apply 阶段已保证单调。
func (t *tracker) metrics(now time.Time) Metrics {
	m := Metrics{
		Partition:   t.partition,
		Regressions: t.regressions,
		Dropped:     t.dropped,
		UpdatedAt:   now,
	}
	if t.write.init {
		m.WriteOffset = t.write.offset
		m.WriteWatermark = t.write.watermark
	}
	if t.consume.init {
		m.ConsumeOffset = t.consume.offset
		m.ConsumeWatermark = t.consume.watermark
		m.FreshnessLag = now.Sub(t.consume.watermark)
		if m.FreshnessLag < 0 {
			m.FreshnessLag = 0
		}
	}
	if t.write.init && t.consume.init {
		if d := t.write.offset - t.consume.offset; d > 0 {
			m.BacklogRecords = d
		}
		if d := t.write.watermark.Sub(t.consume.watermark); d > 0 {
			m.BacklogLag = d
		}
	}
	return m
}
