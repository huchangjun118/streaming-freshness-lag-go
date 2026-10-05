package monitor

import (
	"fmt"
	"sort"
	"time"
)

// Rule 阈值告警规则。任一被启用的阈值越界即触发告警。
type Rule struct {
	ID string
	// FreshnessLagThreshold 新鲜度延迟阈值，<=0 表示不检查。
	FreshnessLagThreshold time.Duration
	// BacklogRecordsThreshold 积压条数阈值，<=0 表示不检查。
	BacklogRecordsThreshold int64
	// BacklogLagThreshold 积压时间阈值，<=0 表示不检查。
	BacklogLagThreshold time.Duration
	// Hysteresis 恢复迟滞比例 [0,1)：指标需回落到
	// 阈值*(1-Hysteresis) 以下才判定恢复，避免阈值附近抖动。
	Hysteresis float64
	// Partitions 限定生效的分区；为空表示对全部分区生效。
	Partitions []PartitionID
}

// matches 判断规则是否适用于指定分区。
func (r Rule) matches(p PartitionID) bool {
	if len(r.Partitions) == 0 {
		return true
	}
	for _, id := range r.Partitions {
		if id == p {
			return true
		}
	}
	return false
}

// breached 检查指标是否越过任一启用阈值，返回越界原因。
func (r Rule) breached(m Metrics) (bool, string) {
	if r.FreshnessLagThreshold > 0 && m.FreshnessLag > r.FreshnessLagThreshold {
		return true, fmt.Sprintf("freshness_lag=%s > threshold=%s", m.FreshnessLag, r.FreshnessLagThreshold)
	}
	if r.BacklogRecordsThreshold > 0 && m.BacklogRecords > r.BacklogRecordsThreshold {
		return true, fmt.Sprintf("backlog_records=%d > threshold=%d", m.BacklogRecords, r.BacklogRecordsThreshold)
	}
	if r.BacklogLagThreshold > 0 && m.BacklogLag > r.BacklogLagThreshold {
		return true, fmt.Sprintf("backlog_lag=%s > threshold=%s", m.BacklogLag, r.BacklogLagThreshold)
	}
	return false, ""
}

// recovered 检查指标是否回落到迟滞线以下。
// 所有启用的阈值都回到迟滞线内才算恢复。
func (r Rule) recovered(m Metrics) (bool, string) {
	h := r.Hysteresis
	if h < 0 {
		h = 0
	}
	if h >= 1 {
		h = 0.99
	}
	if r.FreshnessLagThreshold > 0 &&
		m.FreshnessLag > time.Duration(float64(r.FreshnessLagThreshold)*(1-h)) {
		return false, ""
	}
	if r.BacklogRecordsThreshold > 0 &&
		m.BacklogRecords > int64(float64(r.BacklogRecordsThreshold)*(1-h)) {
		return false, ""
	}
	if r.BacklogLagThreshold > 0 &&
		m.BacklogLag > time.Duration(float64(r.BacklogLagThreshold)*(1-h)) {
		return false, ""
	}
	return true, "all metrics back within hysteresis band"
}

// ruleSet 管理运行期可增删改的规则集合。
type ruleSet struct {
	rules map[string]Rule
}

func newRuleSet() *ruleSet {
	return &ruleSet{rules: make(map[string]Rule)}
}

// upsert 新增或覆盖同 ID 规则。
func (s *ruleSet) upsert(r Rule) {
	s.rules[r.ID] = r
}

func (s *ruleSet) remove(id string) bool {
	if _, ok := s.rules[id]; !ok {
		return false
	}
	delete(s.rules, id)
	return true
}

// list 返回按 ID 排序的规则快照，保证输出稳定。
func (s *ruleSet) list() []Rule {
	out := make([]Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
