package monitor

import (
	"testing"
	"time"
)

func TestRuleAlertAndRecovery(t *testing.T) {
	rs := newRuleSet()
	rs.Upsert(Rule{ID: "lag", Partition: "*", MaxLag: 10})

	breach := PartitionMetrics{Partition: "p0", Lag: 20}
	// 第一次越界 -> 告警。
	ev := rs.evaluate(breach, t0)
	if len(ev) != 1 || ev[0].Type != EventAlert {
		t.Fatalf("want 1 alert event, got %+v", ev)
	}
	// 持续越界 -> 不重复告警。
	if ev := rs.evaluate(breach, t0); len(ev) != 0 {
		t.Fatalf("duplicate alert fired: %+v", ev)
	}
	// 恢复 -> 恢复事件。
	ev = rs.evaluate(PartitionMetrics{Partition: "p0", Lag: 3}, t0)
	if len(ev) != 1 || ev[0].Type != EventRecovery {
		t.Fatalf("want 1 recovery event, got %+v", ev)
	}
	// 持续正常 -> 无事件。
	if ev := rs.evaluate(PartitionMetrics{Partition: "p0", Lag: 3}, t0); len(ev) != 0 {
		t.Fatalf("unexpected event: %+v", ev)
	}
	// 再次越界 -> 再次告警（恢复后的告警不得被压制）。
	if ev := rs.evaluate(breach, t0); len(ev) != 1 || ev[0].Type != EventAlert {
		t.Fatalf("re-alert suppressed: %+v", ev)
	}
}

func TestRuleFreshnessThreshold(t *testing.T) {
	rs := newRuleSet()
	rs.Upsert(Rule{ID: "fresh", Partition: "p1", MaxFreshness: 5 * time.Second})

	// 不匹配的分区不触发。
	if ev := rs.evaluate(PartitionMetrics{Partition: "p0", Freshness: time.Minute}, t0); len(ev) != 0 {
		t.Fatalf("unmatched partition fired: %+v", ev)
	}
	ev := rs.evaluate(PartitionMetrics{Partition: "p1", Freshness: time.Minute}, t0)
	if len(ev) != 1 || ev[0].Type != EventAlert {
		t.Fatalf("want alert, got %+v", ev)
	}
}

func TestRuleUpdateTakesEffect(t *testing.T) {
	rs := newRuleSet()
	rs.Upsert(Rule{ID: "lag", Partition: "*", MaxLag: 10})
	rs.evaluate(PartitionMetrics{Partition: "p0", Lag: 20}, t0) // -> alert

	// 运行期调大阈值：旧告警状态清除，重新评估应产生恢复。
	rs.Upsert(Rule{ID: "lag", Partition: "*", MaxLag: 100})
	ev := rs.evaluate(PartitionMetrics{Partition: "p0", Lag: 20}, t0)
	if len(ev) != 1 || ev[0].Type != EventRecovery {
		t.Fatalf("rule update should cause recovery, got %+v", ev)
	}
}

func TestRuleDeleteStopsEvents(t *testing.T) {
	rs := newRuleSet()
	rs.Upsert(Rule{ID: "lag", Partition: "*", MaxLag: 10})
	rs.evaluate(PartitionMetrics{Partition: "p0", Lag: 20}, t0)

	rs.Delete("lag")
	if ev := rs.evaluate(PartitionMetrics{Partition: "p0", Lag: 1000}, t0); len(ev) != 0 {
		t.Fatalf("deleted rule still firing: %+v", ev)
	}
	if len(rs.Rules()) != 0 {
		t.Fatal("rule not deleted")
	}
}
