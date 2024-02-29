package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.TestUtils.{compactionOption, tempDir}
import io.github.leibnizhu.tinylsm.TinyLsm
import org.scalatest.funsuite.AnyFunSuite

class SimpleCompactionTaskTest extends AnyFunSuite {

  test("week2_day2_task1_integration") {
    val storage = TinyLsm(
      tempDir(),
      compactionOption(CompactionOptions.SimpleCompactionOptions(200, 2, 3))
    )
  }
}
