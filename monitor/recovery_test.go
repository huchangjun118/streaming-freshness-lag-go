package monitor

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestRestartRecovery(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	cfg := Config{CheckpointPath: path, Now: clk.now}

	e1 := NewEngine(cfg)
	e1.AddRule(Rule{ID: "r1", BacklogRecordsThreshold: 1000})
	base := clk.t
	for i := 1; i <= 5; i++ {
		e1.Ingest(wp("p0", int64(i*100), base, uint64(i)))
	}
	e1.Ingest(cp("p0", 200, base, 1))
	if err := e1.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// 模拟崩溃重启：新引擎从检查点恢复。
	e2 := NewEngine(cfg)
	m, ok := e2.Metrics("p0")
	if !ok {
		t.Fatal("恢复后分区状态丢失")
	}
	if m.WriteOffset != 500 || m.ConsumeOffset != 200 {
		t.Fatalf("offsets = %d/%d, want 500/200", m.WriteOffset, m.ConsumeOffset)
	}
	if len(e2.Rules()) != 1 {
		t.Fatal("规则未恢复")
	}

	// 重启后重放已提交的序号：必须被去重，不得重复统计。
	for i := 1; i <= 5; i++ {
		e2.Ingest(wp("p0", int64(i*100), base, uint64(i)))
	}
	m, _ = e2.Metrics("p0")
	if m.WriteOffset != 500 {
		t.Fatalf("重放导致重复统计: write=%d", m.WriteOffset)
	}

	// 继续新进度：不得漏掉区间。
	e2.Ingest(wp("p0", 600, base, 6))
	m, _ = e2.Metrics("p0")
	if m.WriteOffset != 600 || m.BacklogRecords != 400 {
		t.Fatalf("write=%d backlog=%d, want 600/400", m.WriteOffset, m.BacklogRecords)
	}
}

func TestMultiSubscriberSlowDoesNotBlockFast(t *testing.T) {
	e, clk := newTestEngine(t, Config{})
	e.AddRule(Rule{ID: "r1", BacklogRecordsThreshold: 10})

	fast := e.SubscribeWithBuffer(512)
	slow := e.SubscribeWithBuffer(1) // 慢订阅方：小缓冲且不消费

	// 连续摄取触发大量告警状态翻转（阈值上下交替）。
	for i := 1; i <= 200; i++ {
		off := int64(i * 100)
		e.Ingest(wp("p0", off, clk.t, uint64(i)))
		e.Ingest(cp("p0", off, clk.t, uint64(i))) // 追上 -> recovery
	}

	// 快订阅方应收到全部判定。
	count := 0
	for {
		select {
		case <-fast.C:
			count++
		default:
			goto drained
		}
	}
drained:
	// 首轮消费侧未初始化产出 1 条 normal，之后每轮 alert + recovery。
	if want := 1 + 2*199; count != want {
		t.Fatalf("快订阅方收到 %d 条, want %d", count, want)
	}
	if fast.Dropped() != 0 {
		t.Fatalf("快订阅方不应有丢弃: %d", fast.Dropped())
	}
	// 慢订阅方被丢弃，但不得影响摄取与快订阅方。
	if slow.Dropped() == 0 {
		t.Fatal("慢订阅方应产生丢弃")
	}
	var overflow bool
	for _, d := range e.Drops() {
		if d.Reason == DropReasonSubscriberOverflow {
			overflow = true
		}
	}
	if !overflow {
		t.Fatal("应有订阅溢出丢弃记录")
	}
}

func TestPartitionLimit(t *testing.T) {
	e, clk := newTestEngine(t, Config{MaxPartitions: 2})
	e.Ingest(wp("a", 1, clk.t, 1))
	e.Ingest(wp("b", 1, clk.t, 1))
	err := e.Ingest(wp("c", 1, clk.t, 1))
	if !errors.Is(err, ErrPartitionLimit) {
		t.Fatalf("err = %v, want ErrPartitionLimit", err)
	}
	// 已有分区不受影响。
	if err := e.Ingest(wp("a", 2, clk.t, 2)); err != nil {
		t.Fatalf("已有分区摄取失败: %v", err)
	}
	if len(e.AllMetrics()) != 2 {
		t.Fatalf("分区数 = %d, want 2", len(e.AllMetrics()))
	}
	var found bool
	for _, d := range e.Drops() {
		if d.Reason == DropReasonPartitionLimit && d.Partition == "c" {
			found = true
		}
	}
	if !found {
		t.Fatal("应有分区上限丢弃记录")
	}
}

func TestDedupWindowExpiry(t *testing.T) {
	e, clk := newTestEngine(t, Config{DedupWindow: 64})
	// 推进窗口后，旧序号的上报滑出窗口，被区分记录。
	e.Ingest(wp("p0", 1, clk.t, 1))
	for i := uint64(2); i <= 200; i++ {
		e.Ingest(wp("p0", int64(i), clk.t, i))
	}
	e.Ingest(wp("p0", 1, clk.t, 1)) // 旧序号，无法判定是否重复
	var found bool
	for _, d := range e.Drops() {
		if d.Reason == DropReasonDedupWindow {
			found = true
		}
	}
	if !found {
		t.Fatal("应有去重窗口过期丢弃记录")
	}
	m, _ := e.Metrics("p0")
	if m.WriteOffset != 200 {
		t.Fatalf("过期上报不应改变状态: %d", m.WriteOffset)
	}
	if m.Dropped == 0 {
		t.Fatal("分区级丢弃计数应可见")
	}
}

func TestHighThroughputStability(t *testing.T) {
	e, clk := newTestEngine(t, Config{MaxPartitions: 100})
	sub := e.SubscribeWithBuffer(8)
	defer e.Unsubscribe(sub)
	for i := 0; i < 20000; i++ {
		p := wp(fmt.Sprintf("p%d", i%50), int64(i), clk.t, uint64(i))
		if err := e.Ingest(p); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	if got := len(e.AllMetrics()); got != 50 {
		t.Fatalf("分区数 = %d, want 50", got)
	}
	// 事件日志有界。
	if len(e.Regressions()) > eventLogCap || len(e.Drops()) > eventLogCap+dropLogCap {
		t.Fatal("事件日志无界增长")
	}
}
