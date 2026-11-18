from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RecordType(str, Enum):
    UPDATE = "update"
    COMMIT = "commit"
    ABORT = "abort"


class TxnStatus(str, Enum):
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"


def record_checksum(lsn: int, txn_id: str, type_: str,
                    page_id: Optional[str], image: bytes) -> str:
    payload = b"|".join([
        str(lsn).encode(),
        txn_id.encode(),
        type_.encode(),
        (page_id or "").encode(),
        image or b"",
    ])
    return hashlib.sha256(payload).hexdigest()


def page_checksum(page_id: str, data: bytes, lsn: int) -> str:
    payload = b"|".join([page_id.encode(), data, str(lsn).encode()])
    return hashlib.sha256(payload).hexdigest()


@dataclass
class LogRecord:
    lsn: int
    txn_id: str
    type: RecordType
    page_id: Optional[str] = None
    before_image: bytes = b""
    after_image: bytes = b""
    prev_lsn: Optional[int] = None
    checksum: str = ""
    partial: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.type, str):
            self.type = RecordType(self.type)
        if not self.checksum and not self.partial:
            self.checksum = self.compute_checksum()

    def compute_checksum(self) -> str:
        return record_checksum(
            self.lsn, self.txn_id, self.type.value,
            self.page_id, self.after_image + self.before_image,
        )

    @property
    def checksum_ok(self) -> bool:
        if self.partial:
            return False
        return self.checksum == self.compute_checksum()


def update_record(lsn, txn_id, page_id, before_image, after_image,
                  prev_lsn=None, checksum=None, partial=False):
    rec = LogRecord(
        lsn=lsn, txn_id=txn_id, type=RecordType.UPDATE, page_id=page_id,
        before_image=before_image, after_image=after_image,
        prev_lsn=prev_lsn, checksum=checksum or "", partial=partial,
    )
    return rec


def commit_record(lsn, txn_id, prev_lsn=None, checksum=None, partial=False):
    return LogRecord(
        lsn=lsn, txn_id=txn_id, type=RecordType.COMMIT,
        prev_lsn=prev_lsn, checksum=checksum or "", partial=partial,
    )


def abort_record(lsn, txn_id, prev_lsn=None, checksum=None, partial=False):
    return LogRecord(
        lsn=lsn, txn_id=txn_id, type=RecordType.ABORT,
        prev_lsn=prev_lsn, checksum=checksum or "", partial=partial,
    )


@dataclass
class Page:
    page_id: str
    data: bytes
    lsn: int = 0
    checksum: str = ""
    torn: bool = False

    def __post_init__(self) -> None:
        if not self.checksum and not self.torn:
            self.checksum = self.compute_checksum()

    def compute_checksum(self) -> str:
        return page_checksum(self.page_id, self.data, self.lsn)

    @property
    def checksum_ok(self) -> bool:
        if self.torn:
            return False
        return self.checksum == self.compute_checksum()


def make_page(page_id, data, lsn=0, checksum=None, torn=False):
    return Page(page_id=page_id, data=data, lsn=lsn,
                checksum=checksum or "", torn=torn)


@dataclass
class TxnEntry:
    txn_id: str
    status: TxnStatus
    last_lsn: Optional[int]


@dataclass
class Checkpoint:
    dirty_pages: dict = field(default_factory=dict)
    transactions: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        for txn_id, entry in list(self.transactions.items()):
            if isinstance(entry, tuple):
                status, last_lsn = entry
                self.transactions[txn_id] = TxnEntry(
                    txn_id, TxnStatus(status), last_lsn)
            elif isinstance(entry.status, str):
                entry.status = TxnStatus(entry.status)


def make_checkpoint(dirty_pages=None, transactions=None):
    return Checkpoint(dirty_pages=dict(dirty_pages or {}),
                      transactions=dict(transactions or {}))
