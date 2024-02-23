package io.github.leibnizhu.tinylsm

import org.scalatest.Assertions.{assertResult, assertThrows}
import org.scalatest.Entry

import java.io.File

object TestUtils {

  def checkIterator(expect: List[MemTableEntry], actual: MemTableStorageIterator): Unit = {
    for (expectEntry <- expect) {
      assert(actual.isValid)
      println(s"Expect: ${new String(expectEntry.getKey.bytes)} => ${new String(expectEntry.getValue)}, Actual: ${new String(actual.key())} => ${new String(actual.value())}")
      assertResult(expectEntry.getKey.bytes)(actual.key())
      assertResult(expectEntry.getValue)(actual.value())
      actual.next()
    }
    assert(!actual.isValid)
  }

  def expectIteratorError(actual: MemTableStorageIterator): Unit = {
    assertThrows[Exception] {
      while (actual.isValid) {
        actual.next()
      }
    }
  }

  def entry(k: String, v: String): MemTableEntry = {
    Entry(ByteArrayKey(k.getBytes), v.getBytes)
  }

  def tempDir(): File = {
    val tempDirPath = System.getProperty("java.io.tmpdir") + File.separator + "LsmTest"
    val tempDir = new File(tempDirPath)
    if (!tempDir.exists()) {
      tempDir.mkdirs()
    }
    tempDir
  }
}
