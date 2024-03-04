package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.TestUtils.{checkCompactionRatio, compactionBench, compactionOption, tempDir}
import io.github.leibnizhu.tinylsm.{TinyLsm, keyComparator}
import org.scalatest.funsuite.AnyFunSuite

class LeveledCompactionTaskTest extends AnyFunSuite {


  test("week2_day4_integration") {
    val levelSizeMultiplier = 2
    val level0FileNumCompactionTrigger = 2
    val maxLevels = 4
    val baseLevelSizeMb = 1
    val compactOption = compactionOption(CompactionOptions.LeveledCompactionOptions(levelSizeMultiplier, level0FileNumCompactionTrigger, maxLevels, baseLevelSizeMb))
    val storage = TinyLsm(tempDir(), compactOption)
    compactionBench(storage)
    checkCompactionRatio(storage)

    val state = storage.inner.state
    val snapshot = state.read(_.copy())
  }
}
