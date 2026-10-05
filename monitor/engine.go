package monitor

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrPartitionLimit 分区数超出容量上限，新分区的上报被拒绝。
var ErrPartitionLimit = errors.New("monitor: partition limit reached")

// eventLogCap 回退/丢弃事件日志的最大保留条数（环形覆盖）。
const eventLogCap = 1024

// Config 引擎配置。
type Config struct {
	// MaxPartitions 分区状态容量上限，超出后拒绝新分区并记录丢弃；
	// 0 表示不限制。
	MaxPartitions int
	// DedupWindow 每个 (分区, 方向, 来源) 的去重序号窗口大小；
	// <=0 使用默认值 1024。
	DedupWindow int
	// CheckpointPath 检查点文件路径，为空则不持久化。
	CheckpointPath string
	// SubscriberBuffer 每个订阅方的默认缓冲长度；<=0 使用默认值 64。
	SubscriberBuffer int
	// Now 时间源，便于测试；为空使用 time.Now。
	Now func() time.Time
}

func (c *Config) withDefaults() {
	if c.DedupWindow <= 0 {
		c.DedupWindow = 1024
	}
	if c.SubscriberBuffer <= 0 {
		c.SubscriberBuffer = 64
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Engine 是监控引擎入口，并发安全。
type Engine struct {
	mu  sync.Mutex
	cfg Config
	now func() time.Time

	trackers   map[PartitionID]*tracker
	rules      *ruleSet
	alertState map[PartitionID]map[string]bool // ruleID -> 是否处于告警中
	hub        *hub
	store      checkpointStore

	seq         uint64 // 判定序号
	regressions []Regression
	drops       []DropRecord
}

// NewEngine 创建引擎；若配置了检查点且存在快照，则恢复到
// 最近一次已提交的位置继续。
func NewEngine(cfg Config) *Engine {
	cfg.withDefaults()
	e := &Engine{
		cfg:        cfg,
		now:        cfg.Now,
		trackers:   make(map[PartitionID]*tracker),
		rules:      newRuleSet(),
		alertState: make(map[PartitionID]map[string]bool),
		hub:        newHub(cfg.Now),
	}
	if cfg.CheckpointPath != "" {
		e.store = newFileCheckpointStore(cfg.CheckpointPath)
		if data, err := e.store.Load(); err == nil && len(data) > 0 {
			// 快照要么完整恢复，要么完全不恢复，
			// 不会留下部分更新的分区状态。
			_ = e.restore(data)
		}
	}
	return e
}

// Ingest 摄取一条进度上报，返回判定过程中是否发生拒绝。
func (e *Engine) Ingest(p Progress) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	if p.ReportedAt.IsZero() {
		p.ReportedAt = now
	}
	t, ok := e.trackers[p.Partition]
	if !ok {
		if e.cfg.MaxPartitions > 0 && len(e.trackers) >= e.cfg.MaxPartitions {
			e.appendDrop(DropRecord{
				Partition: p.Partition,
				Reason:    DropReasonPartitionLimit,
				Detail:    fmt.Sprintf("max_partitions=%d", e.cfg.MaxPartitions),
				At:        now,
			})
			return ErrPartitionLimit
		}
		t = newTracker(p.Partition, e.cfg.DedupWindow)
		e.trackers[p.Partition] = t
	}
	res := t.apply(p, now)
	for _, reg := range res.regressions {
		e.appendRegression(reg)
	}
	if res.drop != nil {
		e.appendDrop(*res.drop)
	}
	if res.changed {
		e.evaluateLocked(p.Partition, now)
	}
	return nil
}

// evaluateLocked 对单个分区执行全部匹配规则的判定并发布结果。
func (e *Engine) evaluateLocked(part PartitionID, now time.Time) {
	t, ok := e.trackers[part]
	if !ok {
		return
	}
	m := t.metrics(now)
	state := e.alertState[part]
	if state == nil {
		state = make(map[string]bool)
		e.alertState[part] = state
	}
	for _, r := range e.rules.list() {
		if !r.matches(part) {
			continue
		}
		alerting, evaluated := state[r.ID]
		if alerting {
			if ok, reason := r.recovered(m); ok {
				state[r.ID] = false
				e.emitLocked(part, LevelRecovery, r.ID, m, reason, now)
			}
			continue
		}
		if breached, reason := r.breached(m); breached {
			state[r.ID] = true
			e.emitLocked(part, LevelAlert, r.ID, m, reason, now)
		} else if !evaluated {
			// 首次评估且在阈值内：产出一条 normal 判定，保证可追溯。
			state[r.ID] = false
			e.emitLocked(part, LevelNormal, r.ID, m, "within thresholds", now)
		}
	}
}

func (e *Engine) emitLocked(part PartitionID, level Level, ruleID string, m Metrics, reason string, now time.Time) {
	e.seq++
	e.hub.publish(Verdict{
		Partition: part,
		Level:     level,
		RuleID:    ruleID,
		Metrics:   m,
		Reason:    reason,
		Seq:       e.seq,
		At:        now,
	})
}

// Metrics 返回单个分区的当前快照。
func (e *Engine) Metrics(part PartitionID) (Metrics, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.trackers[part]
	if !ok {
		return Metrics{}, false
	}
	return t.metrics(e.now()), true
}

// AllMetrics 返回全部分区的当前快照。
func (e *Engine) AllMetrics() []Metrics {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Metrics, 0, len(e.trackers))
	for _, t := range e.trackers {
		out = append(out, t.metrics(e.now()))
	}
	return out
}

// AddRule 新增或覆盖规则，立即对现有分区生效。
func (e *Engine) AddRule(r Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules.upsert(r)
	// 规则新增/修改后重置该规则的判定状态，按新阈值重新评估。
	for _, state := range e.alertState {
		delete(state, r.ID)
	}
	now := e.now()
	for part := range e.trackers {
		e.evaluateLocked(part, now)
	}
}

// RemoveRule 删除规则，返回规则是否存在。
func (e *Engine) RemoveRule(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	ok := e.rules.remove(id)
	if ok {
		for _, state := range e.alertState {
			delete(state, id)
		}
	}
	return ok
}

// Rules 返回当前生效的规则列表。
func (e *Engine) Rules() []Rule {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rules.list()
}

// Subscribe 注册一个订阅方，使用配置的默认缓冲。
func (e *Engine) Subscribe() *Subscription {
	return e.SubscribeWithBuffer(e.cfg.SubscriberBuffer)
}

// SubscribeWithBuffer 以指定缓冲长度注册订阅方。
func (e *Engine) SubscribeWithBuffer(buf int) *Subscription {
	return e.hub.subscribe(buf)
}

// Unsubscribe 注销订阅方。
func (e *Engine) Unsubscribe(s *Subscription) {
	e.hub.unsubscribe(s)
}

// Regressions 返回已记录的进度回退事件。
func (e *Engine) Regressions() []Regression {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Regression, len(e.regressions))
	copy(out, e.regressions)
	return out
}

// Drops 返回已记录的丢弃/降级事件（含订阅溢出）。
func (e *Engine) Drops() []DropRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DropRecord, 0, len(e.drops))
	out = append(out, e.drops...)
	out = append(out, e.hub.dropRecords()...)
	return out
}

func (e *Engine) appendRegression(r Regression) {
	e.regressions = appendCapped(e.regressions, r, eventLogCap)
}

func (e *Engine) appendDrop(d DropRecord) {
	e.drops = appendCapped(e.drops, d, eventLogCap)
}

func appendCapped[T any](s []T, v T, cap int) []T {
	if len(s) < cap {
		return append(s, v)
	}
	copy(s, s[1:])
	s[len(s)-1] = v
	return s
}

// Commit 将当前状态原子地写入检查点。
func (e *Engine) Commit() error {
	if e.store == nil {
		return nil
	}
	e.mu.Lock()
	data, err := e.snapshotLocked()
	e.mu.Unlock()
	if err != nil {
		return err
	}
	return e.store.Save(data)
}

// Close 提交一次检查点后关闭引擎。
func (e *Engine) Close() error {
	return e.Commit()
}

// ---- 快照序列化 ----

type snapshot struct {
	Seq        uint64
	Trackers   map[PartitionID]trackerSnapshot
	AlertState map[PartitionID]map[string]bool
	Rules      []Rule
}

type trackerSnapshot struct {
	Write       streamSnapshot
	Consume     streamSnapshot
	Regressions uint64
	Dropped     uint64
}

type streamSnapshot struct {
	Init      bool
	Offset    int64
	Watermark time.Time
	Sources   map[string]dedupSnapshot
}

type dedupSnapshot struct {
	Size uint64
	Max  uint64
	Init bool
	Bits []uint64
}

func (e *Engine) snapshotLocked() ([]byte, error) {
	snap := snapshot{
		Seq:        e.seq,
		Trackers:   make(map[PartitionID]trackerSnapshot, len(e.trackers)),
		AlertState: e.alertState,
		Rules:      e.rules.list(),
	}
	for id, t := range e.trackers {
		snap.Trackers[id] = trackerSnapshot{
			Write:       snapshotStream(t.write),
			Consume:     snapshotStream(t.consume),
			Regressions: t.regressions,
			Dropped:     t.dropped,
		}
	}
	return json.Marshal(snap)
}

func snapshotStream(s streamState) streamSnapshot {
	out := streamSnapshot{
		Init:      s.init,
		Offset:    s.offset,
		Watermark: s.watermark,
		Sources:   make(map[string]dedupSnapshot, len(s.sources)),
	}
	for src, w := range s.sources {
		bits := make([]uint64, len(w.bits))
		copy(bits, w.bits)
		out.Sources[src] = dedupSnapshot{Size: w.size, Max: w.max, Init: w.init, Bits: bits}
	}
	return out
}

func (e *Engine) restore(data []byte) error {
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	e.seq = snap.Seq
	if snap.AlertState != nil {
		e.alertState = snap.AlertState
	}
	for _, r := range snap.Rules {
		e.rules.upsert(r)
	}
	for id, ts := range snap.Trackers {
		e.trackers[id] = &tracker{
			partition:   id,
			write:       restoreStream(ts.Write, e.cfg.DedupWindow),
			consume:     restoreStream(ts.Consume, e.cfg.DedupWindow),
			regressions: ts.Regressions,
			dropped:     ts.Dropped,
		}
	}
	return nil
}

func restoreStream(ss streamSnapshot, window int) streamState {
	s := newStreamState(window)
	s.init = ss.Init
	s.offset = ss.Offset
	s.watermark = ss.Watermark
	for src, ds := range ss.Sources {
		bits := make([]uint64, len(ds.Bits))
		copy(bits, ds.Bits)
		s.sources[src] = &dedupWindow{size: ds.Size, max: ds.Max, init: ds.Init, bits: bits}
	}
	return s
}
