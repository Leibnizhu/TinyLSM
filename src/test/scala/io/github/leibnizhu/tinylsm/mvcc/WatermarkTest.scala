package io.github.leibnizhu.tinylsm.mvcc

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
}
