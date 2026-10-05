package monitor

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func newTestMonitor(t *testing.T, cfg Config) *Monitor {
	t.Helper()
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go m.Run()
	t.Cleanup(m.Close)
	return m
}

func TestMonitorEndToEnd(t *testing.T) {
	m := newTestMonitor(t, Config{})
	m.UpsertRule(Rule{ID: "lag", Partition: "*", MaxLag: 10})
	sub := m.Subscribe(16)

	m.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: 100, EventTime: t0, Seq: 1})
	m.Submit(Report{Partition: "p0", Kind: KindConsume, Offset: 50, Seq: 2})

	waitFor(t, "metrics", func() bool {
		mm, ok := m.Metrics("p0")
		return ok && mm.Lag == 50
	})

	// 订阅方应收到带告警事件的快照。
	var gotAlert bool
	timeout := time.After(2 * time.Second)
	for !gotAlert {
		select {
		case s := <-sub.C():
			for _, e := range s.Events {
				if e.Type == EventAlert && e.RuleID == "lag" {
					gotAlert = true
				}
			}
		case <-timeout:
			t.Fatal("no alert event received by subscriber")
		}
	}
}

func TestMonitorRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	// 第一轮：写入进度并提交。
	m1, err := New(Config{CheckpointPath: path, CommitEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	go m1.Run()
	m1.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: 100, EventTime: t0, Seq: 1})
	m1.Submit(Report{Partition: "p0", Kind: KindConsume, Offset: 40, Seq: 2})
	waitFor(t, "commit", func() bool {
		mm, ok := m1.Metrics("p0")
		return ok && mm.Lag == 60
	})
	m1.Close()

	// 第二轮：从 checkpoint 恢复，状态完整且不重不漏。
	m2, err := New(Config{CheckpointPath: path, CommitEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	go m2.Run()
	defer m2.Close()
	mm, ok := m2.Metrics("p0")
	if !ok || mm.WriteOffset != 100 || mm.ConsumeOffset != 40 {
		t.Fatalf("state not recovered: %+v", mm)
	}
	// 重启后重复投递已提交的上报（相同 Seq）不得重复统计。
	m2.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: 100, EventTime: t0, Seq: 1})
	m2.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: 150, EventTime: t0, Seq: 3})
	waitFor(t, "advance", func() bool {
		mm, _ := m2.Metrics("p0")
		return mm.WriteOffset == 150 && mm.ConsumeOffset == 40
	})
}

func TestMonitorIngestBackpressure(t *testing.T) {
	// 不启动 Run，队列无人消费，必然打满。
	m, err := New(Config{IngestQueueSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		m.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: int64(i), Seq: int64(i + 1)})
	}
	drops := m.Drops()
	if len(drops) == 0 {
		t.Fatal("expected ingest drop records")
	}
	for _, d := range drops {
		if d.Reason != DropIngestQueueFull {
			t.Fatalf("unexpected drop reason: %s", d.Reason)
		}
	}
	close(m.stop) // 不经过 Run，直接清理
}

func TestMonitorPartitionLimit(t *testing.T) {
	m := newTestMonitor(t, Config{MaxPartitions: 2})
	for i := 0; i < 10; i++ {
		m.Submit(Report{Partition: fmt.Sprintf("p%d", i), Kind: KindWrite, Offset: 1, EventTime: t0, Seq: int64(i + 1)})
	}
	waitFor(t, "partition limit drops", func() bool {
		for _, d := range m.Drops() {
			if d.Reason == DropPartitionLimit {
				return true
			}
		}
		return false
	})
	if got := len(m.Snapshot()); got != 2 {
		t.Fatalf("partition count = %d, want 2", got)
	}
}

func TestMonitorSlowSubscriberDoesNotBlock(t *testing.T) {
	m := newTestMonitor(t, Config{})
	slow := m.Subscribe(1)   // 容量 1，从不读取
	fast := m.Subscribe(128) // 正常读取
	defer slow.Close()
	defer fast.Close()

	const n = 200
	for i := 0; i < n; i++ {
		m.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: int64(i + 1), EventTime: t0, Seq: int64(i + 1)})
	}
	// 快订阅方应持续收到快照，不被慢订阅方拖住。
	count := 0
	timeout := time.After(3 * time.Second)
	for count < 50 {
		select {
		case <-fast.C():
			count++
		case <-timeout:
			t.Fatalf("fast subscriber blocked after %d snapshots", count)
		}
	}
	// 慢订阅方的丢帧应被记录。
	waitFor(t, "slow subscriber drop record", func() bool {
		for _, d := range m.Drops() {
			if d.Reason == DropSubscriberSlow {
				return true
			}
		}
		return false
	})
}

func TestMonitorRuntimeRuleChange(t *testing.T) {
	m := newTestMonitor(t, Config{})
	sub := m.Subscribe(64)
	defer sub.Close()

	m.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: 100, EventTime: t0, Seq: 1})
	m.Submit(Report{Partition: "p0", Kind: KindConsume, Offset: 90, Seq: 2})
	waitFor(t, "lag 10", func() bool {
		mm, ok := m.Metrics("p0")
		return ok && mm.Lag == 10
	})

	// 运行期新增规则，下一条上报即按新规则判定。
	m.UpsertRule(Rule{ID: "r1", Partition: "*", MaxLag: 5})
	m.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: 101, EventTime: t0, Seq: 3})

	gotAlert := false
	timeout := time.After(2 * time.Second)
	for !gotAlert {
		select {
		case s := <-sub.C():
			for _, e := range s.Events {
				if e.Type == EventAlert && e.RuleID == "r1" {
					gotAlert = true
				}
			}
		case <-timeout:
			t.Fatal("runtime-added rule did not fire")
		}
	}

	// 删除规则后不再产生事件。
	m.DeleteRule("r1")
	m.Submit(Report{Partition: "p0", Kind: KindWrite, Offset: 500, EventTime: t0, Seq: 4})
	waitFor(t, "processed", func() bool {
		mm, _ := m.Metrics("p0")
		return mm.WriteOffset == 500
	})
	// 排空通道，确认没有 r1 的新事件。
	for {
		select {
		case s := <-sub.C():
			for _, e := range s.Events {
				if e.RuleID == "r1" {
					t.Fatalf("deleted rule still firing: %+v", e)
				}
			}
		default:
			return
		}
	}
}
