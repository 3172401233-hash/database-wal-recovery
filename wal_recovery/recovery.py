from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Dict, List, Optional

from .model import (
    Checkpoint,
    LogRecord,
    Page,
    RecordType,
    TxnStatus,
    make_page,
)


@dataclass
class RecoveryResult:
    pages: Dict[str, Page]
    redo_lsns: List[int]
    undo_lsns: List[int]
    winner_txns: List[str]
    loser_txns: List[str]
    truncated_at: Optional[int]
    truncated_reason: Optional[str]
    truncated: bool


def _scan_log(records: List[LogRecord]):
    """顺序扫描日志，遇到半条记录或校验失败即截断其后全部内容。"""
    valid: List[LogRecord] = []
    truncated_at: Optional[int] = None
    reason: Optional[str] = None
    for rec in records:
        if rec.partial:
            truncated_at, reason = rec.lsn, "partial record at log tail"
            break
        if not rec.checksum_ok:
            truncated_at, reason = rec.lsn, "checksum mismatch"
            break
        valid.append(rec)
    return valid, truncated_at, reason


def _analysis(valid_records: List[LogRecord], checkpoint: Checkpoint):
    """结合检查点事务表与日志前缀，判定赢家/输家事务。"""
    txn_status: Dict[str, TxnStatus] = {
        txn_id: entry.status
        for txn_id, entry in checkpoint.transactions.items()
    }
    txn_last_lsn: Dict[str, Optional[int]] = {
        txn_id: entry.last_lsn
        for txn_id, entry in checkpoint.transactions.items()
    }
    for rec in valid_records:
        txn_status.setdefault(rec.txn_id, TxnStatus.ACTIVE)
        txn_last_lsn[rec.txn_id] = rec.lsn
        if rec.type == RecordType.COMMIT:
            txn_status[rec.txn_id] = TxnStatus.COMMITTED
        elif rec.type == RecordType.ABORT:
            txn_status[rec.txn_id] = TxnStatus.ABORTED

    winners = sorted(t for t, s in txn_status.items()
                     if s == TxnStatus.COMMITTED)
    losers = sorted(t for t, s in txn_status.items()
                    if s in (TxnStatus.ACTIVE, TxnStatus.ABORTED))
    return txn_status, txn_last_lsn, winners, losers


def _redo_horizon(valid_records: List[LogRecord],
                  checkpoint: Checkpoint) -> Optional[int]:
    """redo 起点：脏页表最小 recLSN 与日志最小更新 LSN 的较小者。

    检查点之后才变脏的页不在脏页表中，需要把日志里的更新也纳入；
    重做时用页 LSN 跳过规则保证幂等，多扫描不会产生副作用。
    """
    candidates = list(checkpoint.dirty_pages.values())
    candidates.extend(rec.lsn for rec in valid_records
                      if rec.type == RecordType.UPDATE)
    return min(candidates) if candidates else None


def recover(records: List[LogRecord],
            pages: Dict[str, Page],
            checkpoint: Optional[Checkpoint] = None) -> RecoveryResult:
    """ARIES 风格恢复：分析 -> redo -> undo。

    1. 顺序扫描日志，校验每条记录；半条记录或校验失败处之后全部截断。
    2. 结合检查点事务表判定已提交（赢家）与未提交/已回滚（输家）事务。
    3. 从 redo 起点起物理重做，页 LSN >= 记录 LSN 时跳过（幂等）。
    4. 沿 prev_lsn 链收集输家事务的更新并按 LSN 逆序回滚，
       受影响页通过日志 before/after 镜像重建，事务交错也能正确处理。
    """
    checkpoint = checkpoint or Checkpoint()
    original_pages = copy.deepcopy(pages)
    pages = copy.deepcopy(pages)

    valid_records, truncated_at, reason = _scan_log(records)
    index = {rec.lsn: rec for rec in valid_records}

    txn_status, txn_last_lsn, winners, losers = _analysis(
        valid_records, checkpoint)

    # ---------- REDO ----------
    horizon = _redo_horizon(valid_records, checkpoint)
    redo_lsns: List[int] = []
    applied_by_page: Dict[str, List[LogRecord]] = {}
    updates_by_page: Dict[str, List[LogRecord]] = {}

    if horizon is not None:
        for rec in valid_records:
            if rec.type != RecordType.UPDATE or rec.lsn < horizon:
                continue
            updates_by_page.setdefault(rec.page_id, []).append(rec)
            page = pages.get(rec.page_id)
            if page is not None and page.checksum_ok and page.lsn >= rec.lsn:
                continue
            if page is None:
                page = make_page(rec.page_id, b"", 0)
                pages[rec.page_id] = page
            page.data = rec.after_image
            page.lsn = rec.lsn
            page.torn = False
            page.checksum = page.compute_checksum()
            redo_lsns.append(rec.lsn)
            applied_by_page.setdefault(rec.page_id, []).append(rec)

    # ---------- UNDO ----------
    loser_records: List[LogRecord] = []
    for txn_id in losers:
        lsn = txn_last_lsn.get(txn_id)
        while lsn is not None:
            rec = index.get(lsn)
            if rec is None:
                break
            if rec.type == RecordType.UPDATE:
                loser_records.append(rec)
            lsn = rec.prev_lsn

    undo_lsns = sorted((rec.lsn for rec in loser_records), reverse=True)
    loser_pages = {rec.page_id for rec in loser_records}

    # 受输家事务影响的页用日志镜像重建：以该页最早更新的 before-image
    # 为基础，只按 LSN 顺序重放赢家事务的 after-image。
    # 这等价于物理 before-image 回滚，且在多事务交错写同一页时仍然正确，
    # 同时天然处理页损坏（torn）与页缺失。
    for page_id in loser_pages:
        page_records = sorted(updates_by_page.get(page_id, []),
                              key=lambda r: r.lsn)
        winner_records = [
            rec for rec in page_records
            if txn_status[rec.txn_id] == TxnStatus.COMMITTED
        ]
        page = pages[page_id]
        original = original_pages.get(page_id)

        if not winner_records:
            if original is not None and original.checksum_ok:
                pages[page_id] = copy.deepcopy(original)
                continue
            data = page_records[0].before_image if page_records else b""
            final_lsn = 0
        else:
            data = page_records[0].before_image
            final_lsn = 0
            for rec in winner_records:
                data = rec.after_image
                final_lsn = rec.lsn

        page.data = data
        page.lsn = final_lsn
        page.torn = False
        page.checksum = page.compute_checksum()

    return RecoveryResult(
        pages=pages,
        redo_lsns=redo_lsns,
        undo_lsns=undo_lsns,
        winner_txns=winners,
        loser_txns=losers,
        truncated_at=truncated_at,
        truncated_reason=reason,
        truncated=truncated_at is not None,
    )
