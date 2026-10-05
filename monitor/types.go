// Package monitor 提供流式数据新鲜度与消费延迟的进程内监控能力。
package monitor

import "time"

// ProgressKind 区分进度上报的类型。
type ProgressKind int

const (
	// KindWrite 写入进度（生产者侧）。
	KindWrite ProgressKind = iota
	// KindConsume 消费进度（消费者侧）。
	KindConsume
)

func (k ProgressKind) String() string {
	if k == KindWrite {
		return "write"
	}
	return "consume"
}

// Report 是一次进度上报。两路进度到达顺序不确定，允许重复与乱序。
type Report struct {
	Partition string
	Kind      ProgressKind
	Offset    int64     // 分区内的进度位移，单调递增（异常回退会被记录）
	EventTime time.Time // 进度对应的事件时间（用于水位与新鲜度）
	Seq       int64     // 上报方序列号，用于重启后去重
}

// PartitionMetrics 是某个分区当前的新鲜度与积压延迟快照。
type PartitionMetrics struct {
	Partition     string
	WriteOffset   int64
	ConsumeOffset int64
	Lag           int64         // WriteOffset - ConsumeOffset，不为负
	Freshness     time.Duration // 当前时间 - 写入水位
	Watermark     time.Time     // 写入进度水位，单调不回退
	UpdatedAt     time.Time
}

// Regression 记录一次进度回退/跳变异常，可查询。
type Regression struct {
	Partition string
	Kind      ProgressKind
	Prev      int64
	Got       int64
	At        time.Time
}

// AlertLevel 判定结果等级。
type AlertLevel int

const (
	LevelOK AlertLevel = iota
	LevelAlert
)

// EventType 判定事件类型：区分告警与恢复。
type EventType int

const (
	EventAlert    EventType = iota // 正常 -> 告警
	EventRecovery                  // 告警 -> 正常
)

// Event 是一次可追溯的判定结果。
type Event struct {
	Type      EventType
	RuleID    string
	Partition string
	Metrics   PartitionMetrics
	Reason    string
	At        time.Time
}

// DropReason 区分容量降级/丢弃的原因。
type DropReason string

const (
	DropIngestQueueFull DropReason = "ingest_queue_full"
	DropPartitionLimit  DropReason = "partition_limit"
	DropSubscriberSlow  DropReason = "subscriber_slow"
)

// DropRecord 是一条可区分的降级/丢弃记录。
type DropRecord struct {
	Reason    DropReason
	Partition string
	Detail    string
	At        time.Time
}
