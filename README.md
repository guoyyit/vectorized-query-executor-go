# vectorized-query-executor-go

进程内查询执行器，提供两条结果逐条一致的执行通路：

- **逐行通路**（`ExecuteRow`）：一次一条记录地解释执行，作为对照基线。
- **向量化通路**（`ExecuteVec`）：按批（batch）推进，过滤/投影/聚合在列式
  向量上批量求值，仅对选择向量（selection vector）命中的行求值。

两条通路共享同一份标量语义实现（`ApplyBinOp`/`Compare`/`Neg` 与聚合状态
更新函数），因此在相同输入下结果**逐条一致**。`CrossCheck` 用于回归判定：
它在两条通路上执行同一计划并逐行比对，不一致即报错。

整条链路只在本进程内运行：输入为进程内生成的表（`NewTable`）或本地
JSON-lines 数据文件（`LoadJSONLTable`），不依赖任何外部服务。

## 执行模型

计划（`Plan`）按固定顺序组合算子：

```
scan -> filter -> project -> aggregate -> sort
```

- `Filter`：布尔表达式，结果为 `TRUE` 的行保留，`FALSE`/`NULL` 丢弃。
- `Proj`：投影表达式列表；`nil` 表示保留全部列。
- `GroupBy` + `Aggs`：哈希聚合。输出列为分组键后跟各聚合结果。
- `SortKeys`：作用于**最终输出列**的排序键，升序（`Desc` 可反转）。

### 按批推进与选择向量

向量化通路中，扫描产生固定大小（`BatchSize`）的批次；过滤算子为批次安装
选择向量，**被跳过的行不参与后续算子的求值**，下游算子也永远不会物化这些
行。整批被过滤掉时算子继续拉取下一批，不会停滞或提前结束；返回的批次
至少包含一条有效行，流结束返回 `io.EOF`。

### 分区与并发

输入按行范围被划分成 `Partitions` 个连续分区，最多 `Concurrency` 个分区
并发推进，慢分区不会阻塞其他分区。输出顺序是确定性的：

- 无聚合无排序：按分区序拼接（与单分区逐行结果一致）。
- 排序：每个分区内部排序后做 k 路归并，键相等时按分区序打破平局，
  整体稳定且确定。
- 聚合：每个分区产生部分聚合态，按分区序合并；分组输出按分组键排序
  （NULL 在前），与分区数、并发度无关。

共享状态只有原子计数的内存跟踪器；各分区结果写各自槽位，合并在主
goroutine 完成，无数据竞争（`go test -race` 覆盖）。

## 空值与类型语义

- `NULL` 在算术中传播（`NULL+1 = NULL`），在比较中传播（`NULL=1` 为
  `NULL`），在过滤中视为不满足条件。
- 三值逻辑：`FALSE AND NULL = FALSE`，`TRUE OR NULL = TRUE`，
  `NOT NULL = NULL`，其余按 SQL 规则。
- 排序中 `NULL` 小于一切非空值（升序在前，降序在后），两个 `NULL` 相等。
- 聚合：`COUNT(*)` 计行数，`COUNT(col)` 计非空值；`SUM/AVG/MIN/MAX`
  跳过 `NULL`；全空集合上 `SUM/MIN/MAX` 为 `NULL`，`AVG` 为 `NULL`，
  `COUNT` 为 0。无 `GROUP BY` 且输入为空时仍输出一行（SQL 语义）。
- 数值：`int op int` 保持 `int64` 并做溢出检查；任一操作数为 `float64`
  则提升为浮点。整数除法向零取整，任何除以零报错。

### 可区分的失败原因

| 错误 | 含义 |
|---|---|
| `ErrTypeMismatch` | 类型不一致（如 `int + string`） |
| `ErrOverflow` | 数值越界（加减乘、取负、求和） |
| `ErrDivByZero` | 除以零 |
| `ErrSchema` | 列下标越界 |
| `ErrMemoryLimit` | 超出内存上限（见下） |
| `ErrCanceled` | 执行被取消 |
| `ErrTimeout` | 超过截止时间 |

两条通路在相同输入下产生相同的失败；`CrossCheck` 会验证这一点。

## 内存模型

内存上限由 `Options.MemLimitBytes` 配置（`<=0` 表示不限），由
`MemTracker` 记账。预算在执行的分区间**均分**（每分区 `MemLimitBytes/
Partitions`），并发分区因此不会互相挤占；聚合合并与最终排序等合并阶段
单独使用一份等额预算。总占用有界（不超过约两倍预算），不会无界增长：

- **排序**：缓冲达到预算时把已排序的 run 溢写到 `TempDir` 的临时文件，
  输出阶段做 k 路归并。单行都放不下时报 `ErrMemoryLimit`。不会无界增长，
  也不会静默丢数据。
- **聚合**：按组记账，组数超出预算时报 `ErrMemoryLimit`（既定策略：
  聚合不溢写）。失败是显式的，不会丢数据。
- 表达式临时向量与批次缓冲由 `BatchSize` 约束，不计入预算。

执行结束（成功、失败或取消）都会释放记账内存并删除溢写文件。

## 取消与超时

传入的 `context.Context` 被取消或超时时，所有分区安全中断：取消返回
`ErrCanceled`，超时返回 `ErrTimeout`。取消后不返回任何部分结果，溢写
文件被清理，后台 goroutine 全部回收（重复取消、并发取消幂等）。分区
失败会传播并取消其余分区，最终返回根因错误。

## 配置项（`Options`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `BatchSize` | 1024 | 向量化批次行数 |
| `Partitions` | 1 | 输入分区数 |
| `Concurrency` | 1 | 最大并发分区数 |
| `MemLimitBytes` | 0（不限） | 单次执行内存预算（字节） |
| `TempDir` | `os.TempDir()` | 排序溢写目录 |

在任意批次大小、任意分区划分与任意并发度下，两条通路结果逐条一致
（由 `TestRandomizedCrossCheck`、选项矩阵测试与 `TestDeterministicAcrossRuns`
覆盖）。

## 测试

```
go test ./...        # 单元 + 对照 + 内存/取消场景
go test -race ./...  # 竞态检测
```

覆盖：批次大小变化、空批/空表、整批被过滤、空值传播、类型/溢出/除零
错误、排序稳定性、并发分区、内存上限与溢写、取消/超时与资源回收、
随机化双路对照。
