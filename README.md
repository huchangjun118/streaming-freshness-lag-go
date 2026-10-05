# streaming-freshness-lag-go

进程内的流式数据**新鲜度（freshness）与消费延迟（lag）监控**库。持续观测各数据分区的
写入进度与消费进度，输出每个分区当前的新鲜度与积压延迟，并在指标越界时产出可追溯的
判定结果（告警 / 恢复事件）。整个流程只在本地进程内完成，不依赖任何外部监控或存储服务。

## 快速开始

```go
m, _ := monitor.New(monitor.Config{
    CheckpointPath: "/var/lib/monitor/checkpoint.json", // 可选，用于重启恢复
    CommitEvery:    100,
})
go m.Run()
defer m.Close()

m.UpsertRule(monitor.Rule{ID: "lag", Partition: "*", MaxLag: 1000})
sub := m.Subscribe(64) // 每个订阅方独立按自己的进度读取

m.Submit(monitor.Report{Partition: "p0", Kind: monitor.KindWrite, Offset: 100, EventTime: ts, Seq: 1})
m.Submit(monitor.Report{Partition: "p0", Kind: monitor.KindConsume, Offset: 40, Seq: 2})

mm, _ := m.Metrics("p0") // Lag=60, Freshness=now-watermark, ...
snap := <-sub.C()        // 指标快照 + 告警/恢复事件
```

## 延迟语义

- **Lag（积压延迟）** = `WriteOffset - ConsumeOffset`，下限为 0。
- **Freshness（新鲜度）** = `当前时间 - 写入水位(Watermark)`，即最新写入进度对应事件时间
  距现在的时长；水位未建立时为 0。
- 两路进度（写入 / 消费）的到达**顺序不确定**。内部对每路进度只取历史最大值，
  因此**重复上报与乱序上报都是幂等的**，不会改变按真实进度得到的判定结果。
- `Report.Seq` 为上报方序列号：同一分区已提交过的序列号会被去重，
  用于重启后重复投递的场景（配合 checkpoint 恢复保证不重不漏）。

## 水位与迟到数据策略

- **水位单调不回退**：写入进度的水位只取事件时间的最大值。迟到数据（事件时间早于
  当前水位）照常推进 offset，但不会拉低水位，输出的新鲜度与延迟**不随时间倒流**。
- **进度回退识别**：offset 上报值小于已记录的最大值时判定为异常回退——该值被拒绝，
  并记录为 `Regression`（含分区、方向、前后值、时间），可通过
  `Monitor.Regressions(partition)` 查询。每分区回退记录数受 `MaxRegressions` 约束，
  超出时丢弃最旧记录。

## 失败恢复

- 设置 `CheckpointPath` 后，监控器按 `CommitEvery`（条数）和/或 `CommitInterval`
  （时间）提交 checkpoint，关闭时做最终提交。
- checkpoint 采用**原子写入**（临时文件 + fsync + rename）：任何时刻磁盘上要么是
  旧版本要么是新版本，**不会留下部分更新的分区状态**。
- 重启后从最近一次已提交的快照恢复全部分区状态；由于状态基于 max 语义且按 Seq 去重，
  恢复点之后的重放**不会重复统计，也不会漏掉区间**。

## 告警规则（运行期可调）

- `Rule{ID, Partition, MaxLag, MaxFreshness}`：`Partition` 为 `"*"` 或空表示全部分区；
  阈值 `<= 0` 表示该项不检查。
- `UpsertRule` / `DeleteRule` 在运行中立即生效。修改规则时保留既有告警状态，
  按新阈值继续判定（不再越界则产生恢复事件）。
- 判定结果区分三种状态迁移：**正常**（无事件）、**告警**（OK→越界，产出 `EventAlert`）、
  **恢复**（越界→OK，产出 `EventRecovery`）。只有状态翻转才产出事件：持续越界不重复
  告警，恢复后再次越界会重新告警，重复告警既不被压制也不被放大。

## 多订阅与背压

- `Subscribe(size)` 返回独立订阅，多个订阅方各自以自己的进度读取同一份监控结果。
- 发布是非阻塞的：订阅方缓冲满时**丢弃其最旧一帧**并放入最新帧，同时记录
  `DropSubscriberSlow`。慢订阅方不阻塞快订阅方，也不拖慢摄取。

## 容量与降级策略

所有状态均有界，不随运行时间无界增长；超出容量时按既定策略降级并给出**可区分**的记录
（`Monitor.Drops()`）：

| 场景 | 策略 | 记录 |
|---|---|---|
| 摄取队列满（`IngestQueueSize`） | 丢弃该上报，不阻塞调用方 | `DropIngestQueueFull` |
| 分区数超限（`MaxPartitions`） | 拒绝新分区的上报 | `DropPartitionLimit` |
| 订阅方缓冲满 | 丢弃该订阅方最旧快照 | `DropSubscriberSlow` |
| 回退记录超限（`MaxRegressions`） | 丢弃最旧记录 | （保留最近 N 条） |
| 丢弃记录本身 | 保留最近 `MaxDropRecords` 条 | — |

## 配置项

| 字段 | 默认值 | 说明 |
|---|---|---|
| `IngestQueueSize` | 1024 | 摄取队列容量 |
| `MaxPartitions` | 4096 | 分区数上限 |
| `MaxRegressions` | 64 | 每分区回退记录上限 |
| `MaxDropRecords` | 1024 | 丢弃记录保留上限 |
| `CommitEvery` | 100 | 每处理多少条上报提交一次 checkpoint |
| `CommitInterval` | 0（关闭） | checkpoint 定时提交间隔 |
| `CheckpointPath` | 空（不持久化） | checkpoint 文件路径 |

## 测试

```sh
go test -race ./...
```

覆盖：乱序与重复上报、进度回退与水位的单调性、重启恢复（不重不漏）、
规则运行期增删改与告警/恢复迁移、多订阅与慢订阅背压、摄取队列与分区数上限降级。
