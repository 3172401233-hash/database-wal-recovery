import copy
import unittest

from wal_recovery import (
    LogRecord,
    RecordType,
    TxnStatus,
    TxnEntry,
    abort_record,
    commit_record,
    make_checkpoint,
    make_page,
    recover,
    update_record,
)


def clean_shutdown_oracle(records, intact_pages, valid_lsns=None):
    """独立模拟“正常关闭”后的页状态，作为恢复结果的对照基准。

    规则：从完好的初始页出发，只按 LSN 顺序应用已提交事务的更新；
    没有已提交更新的页保持其初始内容。
    """
    valid = [r for r in records
             if not r.partial and r.checksum_ok and (valid_lsns is None
                                                     or r.lsn in valid_lsns)]
    committed = {r.txn_id for r in valid if r.type == RecordType.COMMIT}
    pages = copy.deepcopy(intact_pages)
    touched = {r.page_id for r in valid
               if r.type == RecordType.UPDATE and r.txn_id in committed}
    for page_id in touched:
        page_records = sorted(
            (r for r in valid
             if r.type == RecordType.UPDATE
             and r.page_id == page_id and r.txn_id in committed),
            key=lambda r: r.lsn)
        all_on_page = sorted(
            (r for r in valid
             if r.type == RecordType.UPDATE and r.page_id == page_id),
            key=lambda r: r.lsn)
        data = all_on_page[0].before_image
        final_lsn = 0
        for rec in page_records:
            data = rec.after_image
            final_lsn = rec.lsn
        pages[page_id] = make_page(page_id, data, final_lsn)
    return pages


def assert_pages_match(test_case, result_pages, oracle_pages):
    test_case.assertEqual(set(result_pages), set(oracle_pages))
    for page_id, expected in oracle_pages.items():
        actual = result_pages[page_id]
        test_case.assertEqual(actual.data, expected.data, page_id)
        test_case.assertEqual(actual.lsn, expected.lsn, page_id)
        test_case.assertTrue(actual.checksum_ok, page_id)


class RecoveryTest(unittest.TestCase):
    def test_interleaved_committed_and_uncommitted(self):
        # T1 与 T2 在同一页上交错写入，T1 提交、T2 未提交
        records = [
            update_record(10, "T1", "P1", b"v0", b"v1"),
            update_record(20, "T2", "P1", b"v1", b"v2", prev_lsn=None),
            update_record(30, "T1", "P1", b"v2", b"v3", prev_lsn=10),
            commit_record(40, "T1", prev_lsn=30),
            update_record(50, "T2", "P2", b"w0", b"w2", prev_lsn=20),
        ]
        intact = {"P1": make_page("P1", b"v0", 0),
                  "P2": make_page("P2", b"w0", 0)}

        result = recover(records, copy.deepcopy(intact))

        self.assertEqual(result.winner_txns, ["T1"])
        self.assertEqual(result.loser_txns, ["T2"])
        self.assertEqual(result.redo_lsns, [10, 20, 30, 50])
        self.assertEqual(result.undo_lsns, [50, 20])
        oracle = clean_shutdown_oracle(records, intact)
        assert_pages_match(self, result.pages, oracle)
        self.assertEqual(result.pages["P1"].data, b"v3")
        self.assertEqual(result.pages["P2"].data, b"w0")

    def test_checkpoint_dirty_pages_and_flushed_skips(self):
        # 检查点前的更新已部分刷盘，检查点后继续修改
        records = [
            update_record(10, "T3", "P1", b"a0", b"a1"),
            update_record(20, "T4", "P3", b"c0", b"c1"),
            # ---- checkpoint at LSN 20 ----
            update_record(30, "T3", "P1", b"a1", b"a2", prev_lsn=10),
            commit_record(40, "T3", prev_lsn=30),
            update_record(50, "T4", "P3", b"c1", b"c2", prev_lsn=20),
        ]
        checkpoint = make_checkpoint(
            dirty_pages={"P1": 10, "P3": 20},
            transactions={
                "T3": TxnEntry("T3", TxnStatus.ACTIVE, 10),
                "T4": TxnEntry("T4", TxnStatus.ACTIVE, 20),
            })
        # P1 已刷到 LSN10；P3 只刷到更早状态（LSN0），需要重做 LSN20
        disk = {"P1": make_page("P1", b"a1", 10),
                "P3": make_page("P3", b"c0", 0)}

        result = recover(records, copy.deepcopy(disk), checkpoint)

        self.assertEqual(result.winner_txns, ["T3"])
        self.assertEqual(result.loser_txns, ["T4"])
        # LSN10 因页 LSN 已等于 10 而跳过
        self.assertEqual(result.redo_lsns, [20, 30, 50])
        self.assertEqual(result.undo_lsns, [50, 20])
        oracle = clean_shutdown_oracle(records, disk)
        assert_pages_match(self, result.pages, oracle)
        self.assertEqual(result.pages["P1"].data, b"a2")
        self.assertEqual(result.pages["P3"].data, b"c0")

    def test_torn_page_write_interrupted(self):
        # 页刷盘中断产生 torn 页，校验失败，必须由日志镜像重建
        records = [
            update_record(10, "T5", "P1", b"s0", b"s1"),
            update_record(20, "T6", "P1", b"s1", b"s2"),
            commit_record(30, "T6", prev_lsn=20),
        ]
        intact_p1 = make_page("P1", b"s0", 0)
        torn_p1 = make_page("P1", b"garbage", 20,
                            checksum="deadbeef", torn=True)
        disk = {"P1": torn_p1}

        result = recover(records, disk, make_checkpoint(
            dirty_pages={"P1": 10}))

        self.assertTrue(result.pages["P1"].checksum_ok)
        oracle = clean_shutdown_oracle(
            records, {"P1": intact_p1})
        assert_pages_match(self, result.pages, oracle)
        self.assertEqual(result.pages["P1"].data, b"s2")

    def test_duplicate_or_already_applied_lsn_is_skipped(self):
        # 页 LSN == 记录 LSN：重复/已应用的重放必须幂等跳过
        records = [
            update_record(20, "T7", "P1", b"d0", b"d1"),
            commit_record(25, "T7", prev_lsn=20),
            update_record(30, "T8", "P2", b"e0", b"e1"),
        ]
        disk = {"P1": make_page("P1", b"d1", 20),
                "P2": make_page("P2", b"e0", 0)}

        result = recover(records, copy.deepcopy(disk))

        self.assertEqual(result.redo_lsns, [30])
        self.assertEqual(result.undo_lsns, [30])
        oracle = clean_shutdown_oracle(records, disk)
        assert_pages_match(self, result.pages, oracle)

    def test_corruption_after_commit_keeps_transaction_committed(self):
        # 提交记录完好，其后的日志损坏：提交仍然有效，损坏点之后截断
        records = [
            update_record(10, "T9", "P1", b"k0", b"k1"),
            commit_record(20, "T9", prev_lsn=10),
            update_record(30, "T10", "P2", b"m0", b"m1"),
            LogRecord(lsn=40, txn_id="T10", type=RecordType.UPDATE,
                      page_id="P2", after_image=b"broken",
                      checksum="0" * 64),
        ]
        intact = {"P1": make_page("P1", b"k0", 0),
                  "P2": make_page("P2", b"m0", 0)}

        result = recover(records, copy.deepcopy(intact))

        self.assertTrue(result.truncated)
        self.assertEqual(result.truncated_at, 40)
        self.assertEqual(result.winner_txns, ["T9"])
        self.assertEqual(result.loser_txns, ["T10"])
        self.assertEqual(result.redo_lsns, [10, 30])
        self.assertEqual(result.undo_lsns, [30])
        oracle = clean_shutdown_oracle(
            records, intact, valid_lsns={10, 20, 30})
        assert_pages_match(self, result.pages, oracle)

    def test_torn_commit_record_forces_rollback(self):
        # 提交记录本身是半条：事务状态不能算提交，必须整体回滚
        records = [
            update_record(10, "T11", "P1", b"q0", b"q1"),
            commit_record(20, "T11", prev_lsn=10, partial=True),
        ]
        intact = {"P1": make_page("P1", b"q0", 0)}

        result = recover(records, copy.deepcopy(intact))

        self.assertTrue(result.truncated)
        self.assertEqual(result.truncated_reason,
                         "partial record at log tail")
        self.assertEqual(result.winner_txns, [])
        self.assertEqual(result.loser_txns, ["T11"])
        self.assertEqual(result.undo_lsns, [10])
        oracle = clean_shutdown_oracle(
            records, intact, valid_lsns={10})
        assert_pages_match(self, result.pages, oracle)
        self.assertEqual(result.pages["P1"].data, b"q0")

    def test_partial_tail_record_is_truncated(self):
        records = [
            update_record(10, "T12", "P1", b"z0", b"z1"),
            commit_record(20, "T12", prev_lsn=10),
            update_record(30, "T13", "P1", b"z1", b"z2", partial=True),
        ]
        intact = {"P1": make_page("P1", b"z0", 0)}

        result = recover(records, copy.deepcopy(intact))

        self.assertTrue(result.truncated)
        self.assertEqual(result.truncated_at, 30)
        self.assertEqual(result.winner_txns, ["T12"])
        # T13 在截断前缀中没有任何有效记录，不进入输家列表
        self.assertEqual(result.loser_txns, [])
        oracle = clean_shutdown_oracle(
            records, intact, valid_lsns={10, 20})
        assert_pages_match(self, result.pages, oracle)

    def test_missing_page_is_built_from_log_images(self):
        # 页完全缺失（如尚未建页即断电），由 before/after 镜像重建
        records = [
            update_record(10, "T14", "P9", b"", b"n1"),
            commit_record(20, "T14", prev_lsn=10),
        ]
        result = recover(records, {})

        self.assertEqual(result.pages["P9"].data, b"n1")
        self.assertEqual(result.pages["P9"].lsn, 10)
        self.assertTrue(result.pages["P9"].checksum_ok)

    def test_aborted_transaction_is_rolled_back(self):
        records = [
            update_record(10, "T15", "P1", b"o0", b"o1"),
            abort_record(20, "T15", prev_lsn=10),
            update_record(30, "T16", "P1", b"o0", b"o2"),
            commit_record(40, "T16", prev_lsn=30),
        ]
        intact = {"P1": make_page("P1", b"o0", 0)}

        result = recover(records, copy.deepcopy(intact))

        self.assertEqual(result.winner_txns, ["T16"])
        self.assertEqual(result.loser_txns, ["T15"])
        self.assertEqual(result.undo_lsns, [10])
        oracle = clean_shutdown_oracle(records, intact)
        assert_pages_match(self, result.pages, oracle)
        self.assertEqual(result.pages["P1"].data, b"o2")

    def test_input_pages_and_records_are_not_mutated(self):
        records = [
            update_record(10, "T17", "P1", b"x0", b"x1"),
        ]
        disk = {"P1": make_page("P1", b"x0", 0)}
        records_snapshot = copy.deepcopy(records)
        disk_snapshot = copy.deepcopy(disk)

        recover(records, disk)

        self.assertEqual(records, records_snapshot)
        self.assertEqual(disk, disk_snapshot)


if __name__ == "__main__":
    unittest.main()
