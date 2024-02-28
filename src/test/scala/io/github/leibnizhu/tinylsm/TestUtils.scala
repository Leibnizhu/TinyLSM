package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.iterator.{MergeIterator, SsTableIterator}
import io.github.leibnizhu.tinylsm.block.{BlockCache}
import org.scalatest.Assertions.{assertResult, assertThrows}
import org.scalatest.Entry

import java.io.File
import scala.collection.mutable.ArrayBuffer

object TestUtils {
  val TS_ENABLED = false

  def checkIterator(expect: List[MemTableEntry], actual: MemTableStorageIterator): Unit = {
    for (expectEntry <- expect) {
      assert(actual.isValid)
      println(s"Expect: ${new String(expectEntry.getKey.bytes)} => ${new String(expectEntry.getValue)}, Actual: ${new String(actual.key())} => ${new String(actual.value())}")
      assertResult(new String(expectEntry.getKey.bytes))(new String(actual.key()))
      assertResult(new String(expectEntry.getValue))(new String(actual.value()))
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

  def generateSst(id: Int, path: File, data: Seq[MemTableEntry], blockCache: Option[BlockCache]): SsTable = {
    val builder = SsTableBuilder(128)
    for (entry <- data) {
      builder.add(entry.getKey.bytes, entry.getValue)
    }
    builder.build(id, blockCache, path)
  }

  def syncStorage(storage: LsmStorageInner): Unit = {
    storage.forceFreezeMemTable()
    storage.forceFlushNextImmutableMemTable()
  }

  def constructMergeIteratorOverStorage(state: LsmStorageState): MergeIterator[SsTableIterator] = {
    val iters = new ArrayBuffer[SsTableIterator]()
    for (t <- state.l0SsTables) {
      iters += SsTableIterator.createAndSeekToFirst(state.ssTables(t))
    }
    for (level <- state.levels) {
      for (f <- level._2) {
        iters += SsTableIterator.createAndSeekToFirst(state.ssTables(f))
      }
    }
    MergeIterator(iters.toList)
  }
}
