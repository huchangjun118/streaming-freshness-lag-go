package monitor

import (
	"fmt"
	"sync"
	"time"
)

// Config 监控器配置项。
type Config struct {
	IngestQueueSize int           // 摄取队列容量，满时按策略丢弃
	MaxPartitions   int           // 分区数上限，超出拒绝并记录
	MaxRegressions  int           // 每分区保留的回退记录上限
	MaxDropRecords  int           // 保留的丢弃记录上限
	CommitEvery     int           // 每处理多少条上报提交一次 checkpoint
	CommitInterval  time.Duration // 或每隔多久提交一次
	CheckpointPath  string        // checkpoint 文件路径，空表示不持久化
}

func (c Config) withDefaults() Config {
	if c.IngestQueueSize <= 0 {
		c.IngestQueueSize = 1024
	}
	if c.MaxPartitions <= 0 {
		c.MaxPartitions = 4096
	}
	if c.MaxRegressions <= 0 {
		c.MaxRegressions = 64
	}
	if c.MaxDropRecords <= 0 {
		c.MaxDropRecords = 1024
	}
	if c.CommitEvery <= 0 {
		c.CommitEvery = 100
	}
	return c
}

// Monitor 是顶层监控器：摄取上报、维护指标、评估规则、扇出结果。
type Monitor struct {
	cfg   Config
	tr    *tracker
	rules *ruleSet
	hub   *hub
	store *checkpointStore

	ingest chan Report
	stop   chan struct{}
	done   chan struct{}

	// mu 保护 tr/rules/drops/sinceCommit，查询与摄取可并发。
	mu          sync.Mutex
	drops       []DropRecord
	sinceCommit int
	committed   int64

	now func() time.Time // 可注入，便于测试
}

// New 创建监控器；若配置了 CheckpointPath 且存在已提交快照，则从其恢复，
// 重启后从最近一次已提交的位置继续，已提交过的上报（按 Seq）不会重复统计。
func New(cfg Config) (*Monitor, error) {
	cfg = cfg.withDefaults()
	m := &Monitor{
		cfg:    cfg,
		tr:     newTracker(cfg.MaxRegressions),
		rules:  newRuleSet(),
		hub:    newHub(),
		ingest: make(chan Report, cfg.IngestQueueSize),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		now:    time.Now,
	}
	if cfg.CheckpointPath != "" {
		m.store = newCheckpointStore(cfg.CheckpointPath)
		c, err := m.store.load()
		if err != nil {
			return nil, err
		}
		if c != nil {
			m.tr.loadState(c.Partitions)
			m.committed = c.Committed
		}
	}
	return m, nil
}

// Submit 提交一条进度上报；摄取队列满时丢弃并记录，绝不阻塞调用方。
func (m *Monitor) Submit(r Report) {
	select {
	case m.ingest <- r:
	default:
		m.recordDrop(DropRecord{
			Reason:    DropIngestQueueFull,
			Partition: r.Partition,
			Detail:    "ingest queue full, report dropped",
			At:        m.now(),
		})
	}
}

// Run 启动摄取循环，直到 Close。应在独立 goroutine 中调用。
func (m *Monitor) Run() {
	defer close(m.done)
	var tick <-chan time.Time
	var ticker *time.Ticker
	if m.cfg.CommitInterval > 0 {
		ticker = time.NewTicker(m.cfg.CommitInterval)
		tick = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-m.stop:
			m.commit()
			return
		case <-tick:
			m.commit()
		case r := <-m.ingest:
			m.process(r)
		}
	}
}

// process 处理一条上报：容量检查 -> 应用 -> 评估规则 -> 扇出。
func (m *Monitor) process(r Report) {
	now := m.now()
	m.mu.Lock()
	if _, exists := m.tr.parts[r.Partition]; !exists && len(m.tr.parts) >= m.cfg.MaxPartitions {
		m.drops = appendDrop(m.drops, m.cfg.MaxDropRecords, DropRecord{
			Reason:    DropPartitionLimit,
			Partition: r.Partition,
			Detail:    fmt.Sprintf("partition limit %d reached, report rejected", m.cfg.MaxPartitions),
			At:        now,
		})
		m.mu.Unlock()
		return
	}
	m.tr.apply(r, now)
	metrics, _ := m.tr.metrics(r.Partition, now)
	events := m.rules.evaluate(metrics, now)
	m.sinceCommit++
	needCommit := m.sinceCommit >= m.cfg.CommitEvery
	if needCommit {
		m.commitLocked()
	}
	m.mu.Unlock()

	drops := m.hub.publish(Snapshot{Metrics: []PartitionMetrics{metrics}, Events: events})
	if len(drops) > 0 {
		m.mu.Lock()
		for _, d := range drops {
			m.drops = appendDrop(m.drops, m.cfg.MaxDropRecords, d)
		}
		m.mu.Unlock()
	}
}

// Close 停止监控器并做最终 checkpoint 提交。
func (m *Monitor) Close() {
	close(m.stop)
	<-m.done
	m.hub.close()
}

// commit 提交 checkpoint（若启用）。
func (m *Monitor) commit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitLocked()
}

func (m *Monitor) commitLocked() {
	if m.store == nil {
		m.sinceCommit = 0
		return
	}
	m.committed += int64(m.sinceCommit)
	m.sinceCommit = 0
	_ = m.store.save(&checkpoint{
		Partitions: m.tr.dumpState(),
		Committed:  m.committed,
	})
}

func (m *Monitor) recordDrop(d DropRecord) {
	m.mu.Lock()
	m.drops = appendDrop(m.drops, m.cfg.MaxDropRecords, d)
	m.mu.Unlock()
}

// appendDrop 追加丢弃记录，超出上限时丢弃最旧的，保证内存有界。
func appendDrop(drops []DropRecord, max int, d DropRecord) []DropRecord {
	drops = append(drops, d)
	if len(drops) > max {
		drops = append([]DropRecord(nil), drops[len(drops)-max:]...)
	}
	return drops
}

// Metrics 查询某分区当前指标。
func (m *Monitor) Metrics(partition string) (PartitionMetrics, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tr.metrics(partition, m.now())
}

// Snapshot 返回所有分区指标。
func (m *Monitor) Snapshot() []PartitionMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tr.snapshot(m.now())
}

// Regressions 查询某分区的回退记录。
func (m *Monitor) Regressions(partition string) []Regression {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tr.regressions(partition)
}

// Drops 返回降级/丢弃记录。
func (m *Monitor) Drops() []DropRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DropRecord(nil), m.drops...)
}

// UpsertRule 运行期新增/修改规则，立即生效。
func (m *Monitor) UpsertRule(r Rule) {
	m.mu.Lock()
	m.rules.Upsert(r)
	m.mu.Unlock()
}

// DeleteRule 运行期删除规则。
func (m *Monitor) DeleteRule(id string) {
	m.mu.Lock()
	m.rules.Delete(id)
	m.mu.Unlock()
}

// Subscribe 订阅监控结果，缓冲容量为 size。
func (m *Monitor) Subscribe(size int) *Subscription {
	return m.hub.subscribe(size)
}
