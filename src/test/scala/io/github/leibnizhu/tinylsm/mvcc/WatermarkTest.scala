package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.TestUtils.{compactionOption, tempDir}
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.NoCompaction
import org.scalatest.funsuite.AnyFunSuite

class WatermarkTest extends AnyFunSuite {

  test("week3_day4_task1_watermark") {
    val watermark = new Watermark
    watermark.addReader(0)
    for (i <- 1 to 1000) {
      watermark.addReader(i)
      assertResult(Some(0))(watermark.watermark())
      assertResult(i + 1)(watermark.numRetainedSnapshots())
    }
    var cnt = 1001
    for (i <- 0 until 500) {
      watermark.removeReader(i)
      assertResult(Some(i + 1))(watermark.watermark())
      cnt -= 1
      assertResult(cnt)(watermark.numRetainedSnapshots())
    }
    for (i <- 1000 to 501 by -1) {
      watermark.removeReader(i)
      assertResult(Some(500))(watermark.watermark())
      cnt -= 1
      assertResult(cnt)(watermark.numRetainedSnapshots())
    }
    watermark.removeReader(500)
    assertResult(None)(watermark.watermark())
    assertResult(0)(watermark.numRetainedSnapshots())
    watermark.addReader(2000)
    watermark.addReader(2000)
    watermark.addReader(2001)
    assertResult(2)(watermark.numRetainedSnapshots())
    assertResult(Some(2000))(watermark.watermark())
    watermark.removeReader(2000)
    assertResult(2)(watermark.numRetainedSnapshots())
    assertResult(Some(2000))(watermark.watermark())
    watermark.removeReader(2000)
    assertResult(1)(watermark.numRetainedSnapshots())
    assertResult(Some(2001))(watermark.watermark())
  }

  test("test_task2_snapshot_watermark") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction).copy(enableWal = true)
    val storage = TinyLsm(dir, options)
    val txn1 = storage.newTxn()
    val txn2 = storage.newTxn()
    storage.put("233", "23333")
    val txn3 = storage.newTxn()
    assertResult(txn1.readTs)(storage.inner.mvcc.get.watermark())
    txn1.rollback()
    assertResult(txn2.readTs)(storage.inner.mvcc.get.watermark())
    txn2.rollback()
    assertResult(txn3.readTs)(storage.inner.mvcc.get.watermark())
    txn3.rollback()
    assertResult(storage.inner.mvcc.get.latestCommitTs())(storage.inner.mvcc.get.watermark())
  }
}
