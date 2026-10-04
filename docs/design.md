# 向量化查询执行引擎设计文档

## 1. 概述

本仓库提供两条查询执行通路，在相同输入下给出逐条一致的记录：

- **逐行通路（行式，Volcano 模型）**：`pkg/exec`，一次处理一行，作为语义对照基准。
- **向量化通路（列式，批次模型）**：`pkg/vexec`，一次处理一个批次（batch），按批推进。

两条通路共享同一套类型系统（`pkg/types`）、表达式实现（`pkg/expr`）和聚合逻辑（`pkg/agg`），
因此 NULL 传播、算术与比较语义天然一致。

## 2. 执行语义

### 2.1 算子链

查询计划（`pkg/engine.Plan`）描述如下算子链：

```
Scan -> Filter -> Project -> (Aggregate | Sort)
```

- **Scan**：扫描表的一个分区（或全部分区的并集）。
- **Filter**：谓词求值，NULL 和 FALSE 都跳过该行；被跳过的行不进入后续算子。
  向量化通路中，Filter 对批次求值后做物理压缩（Compact），被跳过的行从批次中移除。
  整批被跳过时返回空批次（非 nil），通路继续推进，不停滞。
- **Project**：对输入行求值标量表达式，输出新列。
- **Aggregate**：分组 + 聚合函数（COUNT(*)、COUNT、SUM、AVG、MIN、MAX）。
- **Sort**：ORDER BY 排序，稳定排序（键值相同时保持输入顺序）。

### 2.2 逐行通路

逐行通路是单线程的 Volcano 迭代器：`Open` / `Next` / `Close`。
- `Scan` 顺序扫描全部分区（`UnionScanOp`）。
- `Sort` 物化全部行后 `sort.SliceStable`。
- `Aggregate` 物化全部行后哈希分组，按编码键排序输出。

### 2.3 向量化通路

向量化通路按批推进：`Open` / `Next`（返回 `*vector.Batch`）/ `Close`。
- `Scan` 按批次大小产出列批次。
- `Filter` 对批次求值谓词，压缩后输出。
- `Project` 对批次求值表达式，输出新批次。
- `Sort` 物化批次，排序后按需溢写磁盘，产出单个有序 Run。
- `Aggregate` 每分区维护局部聚合表，全部完成后按分区顺序合并部分状态。

### 2.4 确定性与逐条一致

- **排序稳定性**：两条通路都使用稳定排序。逐行通路 `sort.SliceStable`；
  向量化通路在批次内稳定排序，跨分区归并时以（分区号，分区内序号）为次序键，
  保证与逐行通路的全局扫描顺序一致。
- **聚合输出顺序**：分组按编码键字典序输出，确定。
- **浮点求和**：浮点加法不满足结合律。逐行通路按扫描顺序逐个累加（左折叠）；
  向量化通路按分区计算部分和，再按分区顺序合并。两种 parenthesization 可能导致
  末位舍入差异（相对误差约 1e-15）。交叉校验对 float64 使用 1e-9 相对容差，
  整数与 NULL 逐位精确比较。

## 3. NULL 语义

### 3.1 比较

- NULL 参与比较（`=`、`<>`、`<`、`<=`、`>`、`>=`）结果为 NULL（三值逻辑）。
- Filter 谓词：NULL 视为"不通过"，该行被跳过。
- 排序：NULL 排在非 NULL 之前；NULL 与 NULL 相等。

### 3.2 算术

- NULL 参与算术（+、-、*、/）结果为 NULL（传播）。
- 整数溢出返回 `ErrOverflow`；除零返回 `ErrDivByZero`。
- 类型不一致返回 `ErrTypeMismatch`。

### 3.3 聚合

- `COUNT(*)`：统计所有行（含 NULL）。
- `COUNT(expr)`：统计 expr 非 NULL 的行。
- `SUM(expr)`：忽略 NULL；无非 NULL 输入时返回 NULL。
- `AVG(expr)`：忽略 NULL；无非 NULL 输入时返回 NULL。
- `MIN` / `MAX`：忽略 NULL；无非 NULL 输入时返回 NULL。
- 分组键中 NULL 自成一组（NULL 与 NULL 相等）。

## 4. 内存模型

### 4.1 内存记账（Accountant）

`pkg/vexec.Accountant` 维护一个可配置的字节预算（`MemoryBudget`）。
算子在缓冲行之前预留（Reserve）内存，释放时归还（Release）。
预算为 0 表示不记账（无界）。

### 4.2 排序溢写（Sort Spill）

- `SortOp` 物化输入批次时预留内存。当预留失败（超出预算）时：
  1. 将当前缓冲区排序，写入磁盘溢写文件（gob 帧格式）。
  2. 释放缓冲区内存，继续处理后续批次。
- 输入结束后，若有溢写文件，将最终缓冲区也溢写，然后对所有溢写 Run 做 K 路归并。
- 归并按层次分组（每组最多 8 个 Run），每组输出时若超出预算则溢写输出文件。
- 最终产出单个有序 Run（内存或磁盘文件）。
- 溢写文件在 `Close` 或查询结束时清理。

### 4.3 聚合配额（Aggregate Quota）

- 聚合哈希表为每个分组预留内存（键长 + 128 字节开销）。
- 预留失败时返回 `ErrMemoryQuotaExceeded`（可区分的失败原因），
  不静默丢弃数据。

### 4.4 溢写文件目录

- `SpillDir` 管理溢写文件。`SpillDir` 为空时创建临时目录并在清理时删除；
  指定路径时在该目录下创建 `.spill` 文件，清理时删除所有 `.spill` 文件。
- 查询取消或失败时，`defer spill.Cleanup()` 确保溢写文件被回收。

## 5. 配置项

`pkg/engine.Config`：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `BatchSize` | int | 1024 | 向量化批次大小（行数） |
| `MemoryBudget` | int64 | 64 MiB | 内存预算（字节），0 表示无界 |
| `Parallelism` | int | runtime.NumCPU | 并发分区 worker 数 |
| `SpillDir` | string | "" | 溢写文件目录，空表示临时目录 |
| `Timeout` | time.Duration | 0 | 查询超时，0 表示无超时 |

## 6. 取消与超时

- `pkg/engine.Handle` 封装 `context.Context` 与取消函数。
- `NewHandle(cfg)` 创建句柄；`Timeout > 0` 时应用超时。
- `Cancel()` 幂等（`sync.Once`），可重复、并发调用。
- 所有算子在循环中检查 `ctx.Err()`，取消后尽快停止。
- 取消后返回 `context.Canceled`（或 `context.DeadlineExceeded`），不返回部分结果。
- 查询结束时 `defer spill.Cleanup()` 回收溢写文件；worker goroutine 通过 `WaitGroup` 等待结束。

## 7. 并发模型

- 向量化通路使用 worker 池（`startWorkers`）并发处理分区。
- worker 按序领取分区（共享 `nextPart` 计数器 + 互斥锁），避免乱序导致的死锁。
- 排序：每分区产出有序 Run，归并时 K 路堆合并，无共享状态。
- 聚合：每分区维护局部聚合表（无锁），全部完成后按分区顺序合并部分状态。
- 普通（无排序/聚合）：worker 产出批次到每分区 channel，sequencer 按分区顺序读取，
  保证输出顺序与逐行通路一致。

## 8. 本地数据文件

- `pkg/catalog.LoadCSV` 从本地 CSV 文件加载表。
- CSV 格式：首行列名，次行类型（int64/float64/string/bool），后续数据行；
  空字段表示 NULL。
- `pkg/catalog.GenerateTable` 确定性生成表（同种子、同配置总是产生相同数据）。
