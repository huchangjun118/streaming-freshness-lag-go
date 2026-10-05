// Package monitor 提供流式数据新鲜度与消费延迟的进程内监控能力。
package monitor

import "time"

// PartitionID 标识一个数据分区。
type PartitionID string

// ProgressKind 区分写入进度与消费进度。
type ProgressKind int

const (
	// WriteProgress 写入侧进度（生产端水位）。
	WriteProgress ProgressKind = iota
	// ConsumeProgress 消费侧进度。
	ConsumeProgress
)

func (k ProgressKind) String() string {
	switch k {
	case WriteProgress:
		return "write"
	case ConsumeProgress:
		return "consume"
	default:
		return "unknown"
	}
}

// Progress 是一次进度上报。两路进度（写/消费）到达顺序不确定，
// 可能重复或乱序；Seq 与 Source 用于去重。
type Progress struct {
	Partition PartitionID
	Kind      ProgressKind
	// Offset 当前已处理到的位移（单调递增的预期，回退会被记录）。
	Offset int64
	// EventTime 该进度对应的数据事件时间水位。
	EventTime time.Time
	// Seq 上报方单调递增序号，配合 Source 去重与乱序识别。
	Seq uint64
	// Source 上报来源标识（同一来源的 Seq 构成一个序列）。
	Source string
	// ReportedAt 上报产生的本地时间；为零值时由引擎填充。
	ReportedAt time.Time
}

// Metrics 是某个分区某一时刻的监控快照。
type Metrics struct {
	Partition PartitionID

	WriteOffset   int64
	ConsumeOffset int64

	// WriteWatermark / ConsumeWatermark 为写/消费两侧各自
	// 见过的最大事件时间（水位只前进不回退）。
	WriteWatermark   time.Time
	ConsumeWatermark time.Time

	// FreshnessLag 消费侧数据的新鲜度延迟：now - ConsumeWatermark。
	FreshnessLag time.Duration
	// BacklogRecords 积压条数：WriteOffset - ConsumeOffset（不为负）。
	BacklogRecords int64
	// BacklogLag 积压时间延迟：WriteWatermark - ConsumeWatermark（不为负）。
	BacklogLag time.Duration

	// Regressions 该分区累计识别到的进度回退次数。
	Regressions uint64
	// Dropped 该分区因去重窗口淘汰等原因被丢弃的上报数。
	Dropped uint64

	UpdatedAt time.Time
}

// Level 判定级别。
type Level int

const (
	// LevelNormal 指标在阈值内。
	LevelNormal Level = iota
	// LevelAlert 指标越界，触发告警。
	LevelAlert
	// LevelRecovery 指标从越界恢复到阈值内。
	LevelRecovery
)

func (l Level) String() string {
	switch l {
	case LevelNormal:
		return "normal"
	case LevelAlert:
		return "alert"
	case LevelRecovery:
		return "recovery"
	default:
		return "unknown"
	}
}

// Verdict 是一次判定结果，带有可追溯的原因与指标快照。
type Verdict struct {
	Partition PartitionID
	Level     Level
	// RuleID 触发该判定的规则；无规则参与时为空。
	RuleID  string
	Metrics Metrics
	Reason  string
	// Seq 引擎内单调递增的判定序号，订阅方据此检测丢包。
	Seq uint64
	At  time.Time
}

// Regression 记录一次进度回退异常，可查询。
type Regression struct {
	Partition PartitionID
	Kind      ProgressKind
	// Field 回退发生的字段："offset" 或 "event_time"。
	Field    string
	Previous string
	Current  string
	At       time.Time
}

// DropRecord 记录一次因容量策略产生的丢弃/降级，可查询。
type DropRecord struct {
	Partition PartitionID
	// Reason 丢弃原因，见 DropReason* 常量。
	Reason string
	Detail string
	At     time.Time
}

const (
	// DropReasonPartitionLimit 分区数超出容量上限，新分区被拒绝。
	DropReasonPartitionLimit = "partition_limit"
	// DropReasonDedupWindow 上报落入已淘汰的去重窗口，无法判定是否重复。
	DropReasonDedupWindow = "dedup_window"
	// DropReasonSubscriberOverflow 订阅方消费过慢，判定结果被丢弃。
	DropReasonSubscriberOverflow = "subscriber_overflow"
)
