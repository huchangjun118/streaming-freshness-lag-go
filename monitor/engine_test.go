package monitor

import (
	"testing"
	"time"
)

// fakeClock 可手动推进的时间源。
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestEngine(t *testing.T, cfg Config) (*Engine, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	cfg.Now = clk.now
	return NewEngine(cfg), clk
}

func wp(part string, off int64, et time.Time, seq uint64) Progress {
	return Progress{Partition: PartitionID(part), Kind: WriteProgress, Offset: off, EventTime: et, Seq: seq, Source: "w"}
}

func cp(part string, off int64, et time.Time, seq uint64) Progress {
	return Progress{Partition: PartitionID(part), Kind: ConsumeProgress, Offset: off, EventTime: et, Seq: seq, Source: "c"}
}

func TestOutOfOrderAndDuplicateReports(t *testing.T) {
	e, clk := newTestEngine(t, Config{})
	base := clk.t

	// 乱序到达：seq 3 先于 seq 2；另有重复上报。
	reports := []Progress{
		wp("p0", 100, base.Add(-time.Minute), 1),
		wp("p0", 300, base.Add(-10*time.Second), 3), // 先到的新进度
		wp("p0", 200, base.Add(-30*time.Second), 2), // 乱序迟到
		wp("p0", 300, base.Add(-10*time.Second), 3), // 重复
		cp("p0", 150, base.Add(-50*time.Second), 1),
		cp("p0", 150, base.Add(-50*time.Second), 1), // 重复
		cp("p0", 250, base.Add(-20*time.Second), 2),
	}
	for _, p := range reports {
		if err := e.Ingest(p); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}

	m, ok := e.Metrics("p0")
	if !ok {
		t.Fatal("metrics missing")
	}
	// 结果必须与按真实进度（最大值）计算一致。
	if m.WriteOffset != 300 || m.ConsumeOffset != 250 {
		t.Fatalf("offsets = %d/%d, want 300/250", m.WriteOffset, m.ConsumeOffset)
	}
	if m.BacklogRecords != 50 {
		t.Fatalf("backlog = %d, want 50", m.BacklogRecords)
	}
	wantFresh := clk.t.Sub(base.Add(-20 * time.Second))
	if m.FreshnessLag != wantFresh {
		t.Fatalf("freshness = %v, want %v", m.FreshnessLag, wantFresh)
	}
	wantLag := 10 * time.Second // -10s - (-20s)
	if m.BacklogLag != wantLag {
		t.Fatalf("backlog lag = %v, want %v", m.BacklogLag, wantLag)
	}
	if len(e.Regressions()) != 0 {
		t.Fatalf("乱序迟到不应记为回退: %+v", e.Regressions())
	}
}

func TestProgressRegressionRecordedAndClamped(t *testing.T) {
	e, clk := newTestEngine(t, Config{})
	base := clk.t

	e.Ingest(wp("p0", 100, base, 1))
	e.Ingest(wp("p0", 50, base.Add(-time.Minute), 2)) // 序号前进但进度回退

	m, _ := e.Metrics("p0")
	if m.WriteOffset != 100 {
		t.Fatalf("offset 回退了: %d, want 100", m.WriteOffset)
	}
	if !m.WriteWatermark.Equal(base) {
		t.Fatalf("watermark 回退了: %v", m.WriteWatermark)
	}
	regs := e.Regressions()
	if len(regs) != 2 { // offset 与 event_time 各一次
		t.Fatalf("regressions = %d, want 2", len(regs))
	}
	if m.Regressions != 2 {
		t.Fatalf("metrics.Regressions = %d, want 2", m.Regressions)
	}
}

func TestAlertAndRecoveryTransitions(t *testing.T) {
	e, clk := newTestEngine(t, Config{})
	sub := e.SubscribeWithBuffer(16)

	e.AddRule(Rule{ID: "r1", BacklogRecordsThreshold: 100})

	// 正常：首次评估产出 normal。
	e.Ingest(wp("p0", 50, clk.t, 1))
	e.Ingest(cp("p0", 0, clk.t, 1))
	v := <-sub.C
	if v.Level != LevelNormal {
		t.Fatalf("level = %v, want normal", v.Level)
	}

	// 越界：alert。
	e.Ingest(wp("p0", 200, clk.t, 2))
	v = <-sub.C
	if v.Level != LevelAlert || v.RuleID != "r1" {
		t.Fatalf("level = %v, want alert", v.Level)
	}

	// 持续越界：不得重复告警。
	e.Ingest(wp("p0", 300, clk.t, 3))
	select {
	case v := <-sub.C:
		t.Fatalf("告警中不应重复产出: %+v", v)
	case <-time.After(50 * time.Millisecond):
	}

	// 回落：recovery，且只产出一次。
	e.Ingest(cp("p0", 250, clk.t, 2))
	v = <-sub.C
	if v.Level != LevelRecovery {
		t.Fatalf("level = %v, want recovery", v.Level)
	}
	select {
	case v := <-sub.C:
		t.Fatalf("恢复不应重复产出: %+v", v)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRuleRuntimeUpdateAndDelete(t *testing.T) {
	e, clk := newTestEngine(t, Config{})
	sub := e.SubscribeWithBuffer(16)

	e.Ingest(wp("p0", 500, clk.t, 1))
	e.Ingest(cp("p0", 0, clk.t, 1))

	// 运行期新增规则，立即对已有分区生效。
	e.AddRule(Rule{ID: "r1", BacklogRecordsThreshold: 100})
	v := <-sub.C
	if v.Level != LevelAlert {
		t.Fatalf("新增规则应立即判定: %v", v.Level)
	}

	// 修改阈值使指标回落到限内：重新评估产出 recovery。
	e.AddRule(Rule{ID: "r1", BacklogRecordsThreshold: 1000})
	v = <-sub.C
	if v.Level != LevelRecovery && v.Level != LevelNormal {
		t.Fatalf("修改规则后应重新判定: %v", v.Level)
	}

	// 删除规则后不再产出该规则的判定。
	if !e.RemoveRule("r1") {
		t.Fatal("remove should succeed")
	}
	e.Ingest(wp("p0", 5000, clk.t, 2))
	select {
	case v := <-sub.C:
		t.Fatalf("规则删除后不应再判定: %+v", v)
	case <-time.After(50 * time.Millisecond):
	}
	if len(e.Rules()) != 0 {
		t.Fatal("rules should be empty")
	}
}

func TestFreshnessNeverFlowsBackward(t *testing.T) {
	e, clk := newTestEngine(t, Config{})
	base := clk.t

	e.Ingest(cp("p0", 10, base, 1))
	m1, _ := e.Metrics("p0")

	// 迟到上报携带更老的水位，新鲜度不得因此变好。
	e.Ingest(cp("p0", 5, base.Add(-time.Hour), 2))
	m2, _ := e.Metrics("p0")
	if m2.FreshnessLag < m1.FreshnessLag {
		t.Fatalf("freshness 倒流: %v -> %v", m1.FreshnessLag, m2.FreshnessLag)
	}

	// 时间推进，新鲜度随时间增长。
	clk.advance(time.Minute)
	m3, _ := e.Metrics("p0")
	if m3.FreshnessLag != m1.FreshnessLag+time.Minute {
		t.Fatalf("freshness = %v, want %v", m3.FreshnessLag, m1.FreshnessLag+time.Minute)
	}
}
