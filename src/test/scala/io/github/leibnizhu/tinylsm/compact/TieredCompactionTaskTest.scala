package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.TestUtils.{compactionBench, compactionOption, tempDir}
import io.github.leibnizhu.tinylsm.TinyLsm
import org.scalatest.funsuite.AnyFunSuite

class TieredCompactionTaskTest extends AnyFunSuite {

  test("week2_day3_integration") {
    val maxSizeAmplificationPercent = 200
    val sizeRatio = 1
    val minMergeWidth = 2
    val numTiers = 3
    val compactOption = compactionOption(CompactionOptions.TieredCompactionOptions(maxSizeAmplificationPercent, sizeRatio, minMergeWidth, numTiers))
    val storage = TinyLsm(tempDir(), compactOption)
    compactionBench(storage)

    val state = storage.inner.state
    val snapshot = state.read(_.copy())
    val compactTask = storage.inner.compactionController.generateCompactionTask(snapshot)
    // 不能再压缩了
    assert(compactTask.isEmpty)

    // TODO 压缩参数校验
    
  }

}
