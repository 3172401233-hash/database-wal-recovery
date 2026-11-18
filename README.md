# database-wal-recovery

Python 数据库 WAL（Write-Ahead Log）恢复库，处理检查点、提交边界、
页重放和尾部损坏，实现 ARIES 风格的 **分析 → redo → undo** 三阶段恢复。

## 数据模型

- `LogRecord`：日志记录，含 `lsn`、`txn_id`、类型（`update`/`commit`/`abort`）、
  页号、`before_image`/`after_image`、`prev_lsn`、校验和与 `partial` 标志。
- `Page`：数据页，含 `data`、页 `lsn`、页校验和与 `torn`（刷盘中断）标志。
- `Checkpoint`：检查点，含脏页表 `dirty_pages = {页号: recLSN}`
  和事务表 `transactions = {事务: TxnEntry(状态, last_lsn)}`。
- 所有记录和页都带 SHA-256 校验字段，`checksum_ok` 用于识别损坏。

## 恢复协议

入口为 `wal_recovery.recover(records, pages, checkpoint)`，返回 `RecoveryResult`
（恢复后的页、redo/undo 的 LSN 集合、赢家/输家事务、截断信息）。

### 1. 日志扫描与尾部截断

按 LSN 顺序扫描日志，逐条校验：

- 记录 `partial=True`（半条记录，断电时只写入一部分）→ 在此截断，
  该记录及其后的所有记录都不参与恢复。
- 记录校验和不匹配（内容损坏）→ 同样在此截断。
- 截断点之前的完整日志构成有效前缀，后续处理只基于该前缀。

### 2. 分析（Analysis）

以检查点事务表为初始状态，重放有效前缀中的状态记录：

- 遇到 `commit` → 事务标记为 **已提交（赢家）**。
- 遇到 `abort` → 事务标记为 **已回滚（输家）**。
- 只出现 `update`、没有提交/回滚记录 → 事务保持 **活动（输家）**。

提交记录与损坏位置的判定规则：

- 提交记录本身完整且校验通过，即使**其后**的日志损坏，该事务仍算已提交，
  其更新保留；损坏点之后的其他事务按未提交处理并回滚。
- 提交记录本身是半条或损坏，则提交不成立，该事务按未提交整体回滚。

### 3. 重做（Redo）

- redo 起点取检查点脏页表最小 `recLSN` 与日志中最小更新 LSN 的较小者，
  以覆盖检查点之后才变脏的页。
- 顺序重做有效前缀中的每条 `update`：
  - 页存在、页校验通过且 `页LSN >= 记录LSN` → 跳过（幂等，
    处理重复 LSN / 已刷盘的更新）。
  - 页为 torn（校验失败）或页缺失 → 不信任磁盘内容，用日志 `after_image`
    重建，并重置页校验和。
- 所有重做记录（含未提交事务的更新）都会先应用，再由 undo 阶段撤销，
  与标准 ARIES 的“先重放全部、再回滚未提交”一致。

### 4. 回滚（Undo）

- 对每个输家事务沿 `prev_lsn` 链收集其全部 `update` 记录，
  按 LSN 逆序组成 undo 集合。
- 受输家事务影响的页用日志镜像重建：以该页最早更新的 `before_image`
  为基础，只按 LSN 顺序重放赢家事务的 `after_image`。
  这等价于物理 before-image 回滚，在多事务交错写同一页时仍然正确，
  并天然处理 torn 页与缺失页。
- 恢复后每个页重新计算校验和，最终页内容、页 LSN 与正常关闭
  （只保留已提交事务效果）完全一致。

## 可容忍与不可容忍的损坏

可容忍（恢复结果仍与正常关闭一致）：

- 日志尾部的半条记录、尾部记录校验和损坏：截断后恢复。
- 提交记录之后的日志损坏：提交照常生效，后续未提交事务回滚。
- 数据页刷盘中断（torn page，页校验失败）：由日志镜像重建。
- 数据页缺失：由 before/after 镜像重建。
- 重复 LSN / 更新已刷盘：依靠页 LSN 跳过，重做保持幂等。
- 未提交事务的页写入：redo 后由 undo 撤销，不会污染最终状态。

不可容忍（按 WAL 语义无法恢复，恢复器在损坏点截断而非继续读取脏数据）：

- 有效日志前缀中间出现的损坏：该点之后的记录全部丢弃，
  因为无法确认后续记录的边界与归属。
- 提交记录本身损坏或只写了一半：提交不成立，事务整体回滚。
- 某页的所有相关日志记录都已损坏：没有可用镜像，无法重建该页。

## 使用示例

```python
from wal_recovery import (
    recover, update_record, commit_record, make_page,
)

records = [
    update_record(10, "T1", "P1", b"v0", b"v1"),
    update_record(20, "T2", "P1", b"v1", b"v2"),
    commit_record(30, "T1", prev_lsn=10),
]
pages = {"P1": make_page("P1", b"v0", 0)}
result = recover(records, pages)

result.pages["P1"].data      # b"v1"（T2 未提交，被回滚）
result.winner_txns           # ["T1"]
result.loser_txns            # ["T2"]
result.redo_lsns             # [10, 20]
result.undo_lsns             # [20]
```

## 测试

```bash
python -m unittest
```

覆盖场景：事务交错提交、检查点后修改与部分刷盘、页写入中断（torn page）、
重复/已应用 LSN、提交后损坏、半条提交记录、尾部半条记录截断、页缺失重建、
已回滚事务，以及输入不被修改的不变量；每个场景都与独立的
“正常关闭”模拟器（oracle）逐页比对内容、LSN 与校验和。
