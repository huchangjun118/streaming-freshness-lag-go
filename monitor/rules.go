package monitor

import (
	"fmt"
	"sort"
	"time"
)

// Rule 是一条阈值告警规则，运行期可增删改。
type Rule struct {
	ID           string
	Partition    string        // 精确匹配；"*" 或 "" 表示全部分区
	MaxLag       int64         // 积压延迟阈值，<=0 表示不检查
	MaxFreshness time.Duration // 新鲜度阈值，<=0 表示不检查
}

func (r Rule) matches(partition string) bool {
	return r.Partition == "" || r.Partition == "*" || r.Partition == partition
}

// evaluate 返回该规则下指标是否越界及原因。
func (r Rule) evaluate(m PartitionMetrics) (breach bool, reason string) {
	if r.MaxLag > 0 && m.Lag > r.MaxLag {
		return true, fmt.Sprintf("lag %d > threshold %d", m.Lag, r.MaxLag)
	}
	if r.MaxFreshness > 0 && m.Freshness > r.MaxFreshness {
		return true, fmt.Sprintf("freshness %s > threshold %s", m.Freshness, r.MaxFreshness)
	}
	return false, ""
}

// ruleSet 线程安全的规则集合，并维护每个 (rule, partition) 的告警状态，
// 用于区分告警与恢复、避免重复告警被压制或放大。
type ruleSet struct {
	rules  map[string]Rule
	states map[string]map[string]bool // ruleID -> partition -> alerting
}

func newRuleSet() *ruleSet {
	return &ruleSet{rules: make(map[string]Rule), states: make(map[string]map[string]bool)}
}

// Upsert 新增或修改规则，立即生效。
// 修改规则时保留既有告警状态：下一轮评估按新阈值判定，
// 若已不再越界则产生恢复事件，若仍越界则维持告警不重复发送。
func (s *ruleSet) Upsert(r Rule) {
	s.rules[r.ID] = r
}

// Delete 删除规则，其告警状态一并清除。
func (s *ruleSet) Delete(id string) {
	delete(s.rules, id)
	delete(s.states, id)
}

// Rules 返回当前规则副本（按 ID 排序）。
func (s *ruleSet) Rules() []Rule {
	out := make([]Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// evaluate 对指标评估全部规则，返回状态迁移产生的事件（告警/恢复）。
// 只有状态翻转才产出事件：持续告警不重复发，持续正常不发恢复。
func (s *ruleSet) evaluate(m PartitionMetrics, now time.Time) []Event {
	var events []Event
	for _, r := range s.Rules() {
		if !r.matches(m.Partition) {
			continue
		}
		breach, reason := r.evaluate(m)
		st := s.states[r.ID]
		if st == nil {
			st = make(map[string]bool)
			s.states[r.ID] = st
		}
		alerting := st[m.Partition]
		switch {
		case breach && !alerting:
			st[m.Partition] = true
			events = append(events, Event{
				Type:      EventAlert,
				RuleID:    r.ID,
				Partition: m.Partition,
				Metrics:   m,
				Reason:    reason,
				At:        now,
			})
		case !breach && alerting:
			st[m.Partition] = false
			events = append(events, Event{
				Type:      EventRecovery,
				RuleID:    r.ID,
				Partition: m.Partition,
				Metrics:   m,
				Reason:    "recovered",
				At:        now,
			})
		}
	}
	return events
}
