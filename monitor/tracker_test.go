package monitor

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestTrackerOutOfOrderAndDuplicate(t *testing.T) {
	tr := newTracker(8)
	now := t0.Add(time.Minute)

	// 乱序到达：先大后小，再重复。
	reports := []Report{
		{Partition: "p0", Kind: KindWrite, Offset: 100, EventTime: t0, Seq: 1},
		{Partition: "p0", Kind: KindWrite, Offset: 40, EventTime: t0, Seq: 2},  // 乱序旧值
		{Partition: "p0", Kind: KindWrite, Offset: 100, EventTime: t0, Seq: 3}, // 重复
		{Partition: "p0", Kind: KindConsume, Offset: 30, Seq: 4},
		{Partition: "p0", Kind: KindConsume, Offset: 10, Seq: 5}, // 乱序旧值
		{Partition: "p0", Kind: KindConsume, Offset: 30, Seq: 6}, // 重复
	}
	for _, r := range reports {
		tr.apply(r, now)
	}
	m, ok := tr.metrics("p0", now)
	if !ok {
		t.Fatal("metrics missing")
	}
	if m.WriteOffset != 100 || m.ConsumeOffset != 30 || m.Lag != 70 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestTrackerRegressionRecorded(t *testing.T) {
	tr := newTracker(8)
	now := t0
	tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: 100, EventTime: t0}, now)
	tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: 50, EventTime: t0}, now)

	m, _ := tr.metrics("p0", now)
	if m.WriteOffset != 100 {
		t.Fatalf("regression must not move offset backward, got %d", m.WriteOffset)
	}
	regs := tr.regressions("p0")
	if len(regs) != 1 || regs[0].Prev != 100 || regs[0].Got != 50 {
		t.Fatalf("regression not recorded: %+v", regs)
	}
}

func TestTrackerRegressionBounded(t *testing.T) {
	tr := newTracker(4)
	now := t0
	tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: 1000, EventTime: t0}, now)
	for i := 0; i < 10; i++ {
		tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: int64(i), EventTime: t0}, now)
	}
	if got := len(tr.regressions("p0")); got != 4 {
		t.Fatalf("regressions must be bounded to 4, got %d", got)
	}
}

func TestTrackerWatermarkMonotonic(t *testing.T) {
	tr := newTracker(8)
	now := t0.Add(10 * time.Second)
	// 迟到数据：事件时间早于水位。
	tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: 10, EventTime: t0.Add(9 * time.Second)}, now)
	tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: 11, EventTime: t0.Add(2 * time.Second)}, now)

	m, _ := tr.metrics("p0", now)
	if !m.Watermark.Equal(t0.Add(9 * time.Second)) {
		t.Fatalf("watermark moved backward: %v", m.Watermark)
	}
	if m.Freshness != time.Second {
		t.Fatalf("freshness = %v, want 1s", m.Freshness)
	}
}

func TestTrackerSeqDedup(t *testing.T) {
	tr := newTracker(8)
	tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: 5, EventTime: t0, Seq: 7}, t0)
	_, accepted := tr.apply(Report{Partition: "p0", Kind: KindWrite, Offset: 9, EventTime: t0, Seq: 7}, t0)
	if accepted {
		t.Fatal("duplicate seq must be rejected")
	}
	m, _ := tr.metrics("p0", t0)
	if m.WriteOffset != 5 {
		t.Fatalf("duplicate seq changed state: %+v", m)
	}
}
