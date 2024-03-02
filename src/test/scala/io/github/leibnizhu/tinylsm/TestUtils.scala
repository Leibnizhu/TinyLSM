package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.block.BlockCache
import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.iterator.{MergeIterator, SsTableIterator}
import io.github.leibnizhu.tinylsm.utils.Unbounded
import org.scalatest.Assertions.{assertResult, assertThrows}
import org.scalatest.Entry

import java.io.File
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object TestUtils {
  val TS_ENABLED = false

  def checkIterator(expect: List[MemTableEntry], actual: MemTableStorageIterator, verbose: Boolean = true): Unit = {
    for (expectEntry <- expect) {
      assert(actual.isValid)
      if (verbose) {
        println(s"Expect: ${new String(expectEntry.getKey.bytes)} => ${new String(expectEntry.getValue)}, Actual: ${new String(actual.key())} => ${new String(actual.value())}")
      }
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

  def compactionOption(compactOpt: CompactionOptions): LsmStorageOptions = LsmStorageOptions(
    4096,
    1 << 20,
    2,
    compactOpt,
    false,
    false)

  def compactionBench(storage: TinyLsm): Unit = {
    // 10B
    def genKey(i: Int) = "%010d".format(i)

    // 110B
    def genValue(i: Int) = "%0110d".format(i)

    var maxKey = 0
    var keyMap = mutable.HashMap[Int, Int]()
    val overlaps = if (TS_ENABLED) 10000 else 20000
    var cnt = 0
    for (iter <- 0 until 10) {
      val rangeBegin = iter * 5000
      for (i <- rangeBegin until rangeBegin + overlaps) {
        // 每个key 120B， 4MB
        val key = genKey(i)
        val version = keyMap.getOrElse(i, 0) + 1
        val value = genValue(version)
        keyMap.put(i, version)
        storage.put(key, value)
        maxKey = maxKey.max(i)
        cnt += 1
      }
    }
    println(s"====> total entry: $cnt")

    // 等所有MemTable Flush 完
    TimeUnit.SECONDS.sleep(1)
    while (!storage.inner.state.read(_.immutableMemTables.isEmpty)) {
      storage.inner.forceFlushNextImmutableMemTable()
    }
    println("====> After flush MemTables")
    storage.inner.dumpState()

    var prevSnapshot = storage.inner.state.copy()
    while ( {
      TimeUnit.SECONDS.sleep(1);
      val snapshot = storage.inner.state.copy()
      val toContinue = prevSnapshot.levels != snapshot.levels ||
        prevSnapshot.l0SsTables != snapshot.l0SsTables
      prevSnapshot = snapshot
      toContinue
    }) {
      println("waiting for compaction to converge")
    }
    println("====> After compaction")
    storage.inner.dumpState()
    
    val expectedEntries = new ListBuffer[MemTableEntry]()
    for (i <- 0 until maxKey + 40000) {
      val key = genKey(i)
      val value = storage.get(key)
      keyMap.get(i) match
        case Some(expectKey) => {
          val expectedValue = genValue(expectKey)
          assertResult(expectedValue)(value.get)
          expectedEntries += entry(key, expectedValue)
        }
        case None => assert(value.isEmpty)
    }
    checkIterator(expectedEntries.toList, storage.scan(Unbounded(), Unbounded()), false)
  }
}
