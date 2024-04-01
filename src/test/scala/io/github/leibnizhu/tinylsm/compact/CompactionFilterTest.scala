package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.TestUtils.*
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.NoCompaction
import io.github.leibnizhu.tinylsm.{TinyLsm, WriteBatchRecord}
import org.scalatest.funsuite.AnyFunSuite

class CompactionFilterTest extends AnyFunSuite {

  test("week3_day7_task3_mvcc_compaction") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction)
    val storage = TinyLsm(dir, options)
    storage.writeBatch(Seq(
      WriteBatchRecord.Put("table1_a".getBytes, "1".getBytes),
      WriteBatchRecord.Put("table1_b".getBytes, "1".getBytes),
      WriteBatchRecord.Put("table1_c".getBytes, "1".getBytes),
      WriteBatchRecord.Put("table2_a".getBytes, "1".getBytes),
      WriteBatchRecord.Put("table2_b".getBytes, "1".getBytes),
      WriteBatchRecord.Put("table2_c".getBytes, "1".getBytes),
    ))
    storage.forceFlush()

    val snapshot0 = storage.newTxn()
    storage.writeBatch(Seq(
      WriteBatchRecord.Put("table1_a".getBytes, "2".getBytes),
      WriteBatchRecord.Del("table1_b".getBytes),
      WriteBatchRecord.Put("table1_c".getBytes, "2".getBytes),
      WriteBatchRecord.Put("table2_a".getBytes, "2".getBytes),
      WriteBatchRecord.Del("table2_b".getBytes),
      WriteBatchRecord.Put("table2_c".getBytes, "2".getBytes),
    ))

    storage.forceFlush()
    storage.addCompactionFilter(CompactionFilter.Prefix("table2_".getBytes))
    storage.forceFullCompaction()

    val iter1 = constructMergeIteratorOverStorage(storage.inner.state)
    checkIterator(List(
      entry("table1_a", "2"),
      entry("table1_a", "1"),
      entry("table1_b", ""),
      entry("table1_b", "1"),
      entry("table1_c", "2"),
      entry("table1_c", "1"),
      entry("table2_a", "2"),
      entry("table2_b", ""),
      entry("table2_c", "2"),
    ), iter1)

    snapshot0.rollback()
    storage.forceFullCompaction()

    val iter2 = constructMergeIteratorOverStorage(storage.inner.state)
    checkIterator(List(
      entry("table1_a", "2"),
      entry("table1_c", "2"),
    ), iter2)
  }
}
