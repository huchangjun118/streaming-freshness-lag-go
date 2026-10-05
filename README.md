# streaming-freshness-lag-go

进程内的流式数据新鲜度与消费延迟监控库。持续观测各数据分区的写入进度与
消费进度，输出每个分区当前的新鲜度与积压延迟，并在指标越界时产出可追溯的
判定结果。整个流程只在本地进程内完成，不依赖任何外部监控或存储服务。

## 快速开始

```go
eng := monitor.NewEngine(monitor.Config{
    MaxPartitions:    1000,
    DedupWindow:      1024,
    CheckpointPath:   "/var/lib/myapp/monitor-checkpoint.json",
    SubscriberBuffer: 64,
})

// 注册阈值规则（运行期可随时增删改）。
eng.AddRule(monitor.Rule{
    ID:                      "freshness-slo",
    FreshnessLagThreshold:   30 * time.Second,
    BacklogRecordsThreshold: 100000,
    Hysteresis:              0.1,
})

// 订阅判定结果（可多方订阅，互不影响）。
sub := eng.Subscribe()
go func() {
    for v := range sub.C {
        log.Printf("%s %s rule=%s reason=%s", v.Partition, v.Level, v.RuleID, v.Reason)
    }
}()

// 上报两路进度（顺序任意，可重复、可乱序）。
eng.Ingest(monitor.Progress{Partition: "p0", Kind: monitor.WriteProgress,
    Offset: 1000, EventTime: ts, Seq: 1, Source: "producer-1"})
eng.Ingest(monitor.Progress{Partition: "p0", Kind: monitor.ConsumeProgress,
    Offset: 800, EventTime: ts2, Seq: 1, Source: "consumer-1"})

// 定期提交检查点；重启后自动从最近提交位置恢复。
eng.Commit()
```

## 延迟语义

每个分区的 `Metrics` 包含：

| 字段 | 定义 |
| --- | --- |
| `FreshnessLag` | 新鲜度延迟 = `now - ConsumeWatermark`，即消费侧数据比当前时间落后多少 |
| `BacklogRecords` | 积压条数 = `WriteOffset - ConsumeOffset`（不为负） |
| `BacklogLag` | 积压时间延迟 = `WriteWatermark - ConsumeWatermark`（不为负） |
| `Regressions` / `Dropped` | 该分区累计的回退次数与被丢弃的上报数 |

新鲜度与延迟**只前进不回退**：位移与水位均按 max 语义合并，
任何让指标"变好"的倒灌数据都不会改变已输出的判定基准。

## 乱序、重复与水位策略

- 每条上报携带 `(Source, Seq)`。同一来源的序号构成一个序列，
  引擎用**滑动位图去重窗口**（大小由 `DedupWindow` 决定）区分四种情况：
  - **新上报**（序号前进）：正常合并；若进度值反而回退，记为**回退异常**
    （`Engine.Regressions()` 可查），状态保持不回退。
  - **迟到上报**（窗口内的乱序）：按 max 语义合并，不视为回退。
  - **重复上报**：直接忽略。
  - **过期上报**（序号已滑出窗口）：无法判定是否重复，丢弃并记录
    （`DropReasonDedupWindow`）。
- 水位（`EventTime`）只前进不回退；迟到数据携带的老水位被自然吸收。
- 写/消费两路进度到达顺序任意，任一路先到都能正确计算，
  待另一路到达后补全积压指标。

## 判定与告警

- 规则（`Rule`）支持三个阈值：新鲜度延迟、积压条数、积压时间，
  任一越界即告警；可用 `Partitions` 限定生效分区。
- 判定结果分三级：`normal`（首次评估在阈值内）、`alert`（越界）、
  `recovery`（回落）。告警期间不重复告警，恢复只产出一次，
  不会重复压制或放大。
- `Hysteresis` 为恢复迟滞比例：指标需回落到 `阈值*(1-Hysteresis)`
  以下才判定恢复，避免阈值附近抖动。
- 规则在运行期新增、修改、删除立即生效：新增/修改会按新阈值
  重新评估所有分区，删除后不再产出该规则的判定。

## 失败恢复

- `Commit()` 将全量状态（各分区进度、去重窗口、规则、告警状态、
  判定序号）原子写入检查点文件（临时文件 + rename，崩溃不会留下
  部分写入）。
- 重启后 `NewEngine` 自动从最近一次提交恢复：重放已提交的序号会被
  去重窗口吸收（不重复统计），新序号继续推进（不漏区间）。

## 多订阅与背压

- `Subscribe()`/`SubscribeWithBuffer(n)` 注册订阅方，各自拥有独立
  有界缓冲，互不影响。
- 订阅方消费过慢导致缓冲满时，对该订阅方**丢弃**判定并计数
  （`Subscription.Dropped()`、`DropReasonSubscriberOverflow`），
  不阻塞其他订阅方，也不拖慢摄取路径。

## 容量与降级

- `MaxPartitions` 限制分区状态数量：超限的新分区上报被拒绝
  （返回 `ErrPartitionLimit`），并记录 `DropReasonPartitionLimit`，
  已有分区不受影响。
- 回退与丢弃事件日志均为有界环形缓冲（各 1024 条），
  指标状态不随运行时间无界增长。

## 配置项

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `MaxPartitions` | 0（不限） | 分区状态容量上限 |
| `DedupWindow` | 1024 | 每 (分区,方向,来源) 的去重序号窗口大小 |
| `CheckpointPath` | 空（不持久化） | 检查点文件路径 |
| `SubscriberBuffer` | 64 | 订阅方默认缓冲长度 |
| `Now` | `time.Now` | 时间源（测试用） |

## 测试

```sh
go test ./...
```

覆盖：乱序与重复上报、进度回退识别与钳制、告警/恢复转换、
规则运行期增删改、重启恢复去重、多订阅背压、分区容量上限、
去重窗口过期与高吞吐稳定性。
