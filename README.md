# 向量化查询执行引擎

本仓库提供两条查询执行通路：逐行（Volcano）参考通路与按批向量化通路，
在相同输入下给出逐条一致的记录。整条链路在本进程内运行，输入使用本地数据文件
或进程内生成的表，不依赖任何外部服务。

## 特性

- **双通路对照**：逐行通路（`pkg/exec`）与向量化通路（`pkg/vexec`）共享类型系统、
  表达式与聚合逻辑，交叉校验（`engine.CrossCheck`）保证逐条一致。
- **按批推进**：扫描、过滤、投影、排序、聚合全部按批处理；整批被跳过时通路继续推进。
- **有限内存**：内存预算可配置；排序超限时溢写磁盘（外部归并排序），聚合超限返回
  可区分的 `ErrMemoryQuotaExceeded`，不静默丢弃数据。
- **NULL 语义**：比较、算术、聚合中 NULL 传播规则统一；NULL 分组自成一组。
- **并发分区**：worker 池并发处理分区，按序领取避免死锁；聚合使用局部状态 + 有序合并。
- **取消与超时**：`engine.Handle` 幂等取消；取消后回收溢写文件与后台任务，不返回部分结果。
- **本地数据**：CSV 加载器 + 确定性表生成器。

## 快速开始

```go
cfg := engine.DefaultConfig()
cfg.BatchSize = 1024
cfg.MemoryBudget = 64 << 20
cfg.Parallelism = 4

tbl := catalog.GenerateTable("t", schema, catalog.GeneratorConfig{...})
plan := &engine.Plan{
    Table:  tbl,
    Filter: expr.Gt(expr.Col(2), expr.FloatLit(100)),
    GroupBy: []expr.Expr{expr.Col(1)},
    Aggregates: []agg.Def{
        {Func: agg.CountStar},
        {Func: agg.Sum, Expr: expr.Col(2)},
    },
}

// 逐行通路
rowResult, _ := engine.ExecuteRow(ctx, cfg, plan)

// 向量化通路
vecResult, _ := engine.ExecuteVector(ctx, cfg, plan)

// 交叉校验
check, _ := engine.CrossCheck(ctx, cfg, plan)
if !check.Match {
    log.Fatal(check.Detail)
}
```

## 配置项

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `BatchSize` | 1024 | 向量化批次大小 |
| `MemoryBudget` | 64 MiB | 内存预算（字节），0 表示无界 |
| `Parallelism` | runtime.NumCPU | 并发分区 worker 数 |
| `SpillDir` | "" | 溢写目录，空表示临时目录 |
| `Timeout` | 0 | 查询超时 |

## 测试

```bash
go test ./...              # 全部测试
go test -race ./...        # 含数据竞争检测
```

测试覆盖：批次大小变化、空批、NULL、并发分区、内存上限（排序溢写与聚合配额）、
取消与超时、CSV 加载、确定性生成。

## 文档

详见 [docs/design.md](docs/design.md)。
