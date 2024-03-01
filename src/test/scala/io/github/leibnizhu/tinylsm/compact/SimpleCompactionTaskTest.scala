package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.TestUtils.{compactionBench, compactionOption, tempDir}
import io.github.leibnizhu.tinylsm.TinyLsm
import org.scalatest.funsuite.AnyFunSuite

class SimpleCompactionTaskTest extends AnyFunSuite {

  test("week2_day2_integration") {
    val sizeRatioPercent = 200
    val level0FileNumCompactionTrigger = 2
    val maxLevels = 3
    val compactOption = compactionOption(CompactionOptions.SimpleCompactionOptions(sizeRatioPercent, level0FileNumCompactionTrigger, maxLevels))
    val storage = TinyLsm(tempDir(), compactOption)
    compactionBench(storage)

    val state = storage.inner.state
    val snapshot = state.read(_.copy())
    val compactTask = storage.inner.compactionController.generateCompactionTask(snapshot)
    // 不能再压缩了
    assert(compactTask.isEmpty)
    // 压缩参数校验
    assert(state.levels.length <= maxLevels)
    assert(state.l0SsTables.length < level0FileNumCompactionTrigger)
    for (i <- 0 until maxLevels - 1) {
      val upperSize = state.levels(i)._2.length
      val lowerSize = state.levels(i + 1)._2.length
      assert(upperSize == 0 || 100.0 * lowerSize / upperSize >= sizeRatioPercent)
    }
  }
}
