package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.block.BlockCache
import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.{LeveledCompactionOptions, SimpleCompactionOptions, TieredCompactionOptions}
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

  private val tempDirs = new ListBuffer[File]

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    for (dir <- tempDirs) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }))

  def tempDir(): File = {
    val tempDirPath = System.getProperty("java.io.tmpdir") + File.separator + "LsmTest" + File.separator + System.currentTimeMillis()
    val tempDir = new File(tempDirPath)
    if (!tempDir.exists()) {
      tempDir.mkdirs()
    }
    tempDirs += tempDir
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

  def compactionOption(compactOpt: CompactionOptions, enableWal: Boolean = false): LsmStorageOptions = LsmStorageOptions(
    4096,
    1 << 20,
    2,
    compactOpt,
    enableWal,
    false)

  def compactionBench(storage: TinyLsm): Unit = {
    // 10B
    def genKey(i: Int) = "%010d".format(i)

    // 110B
    def genValue(i: Int) = "%0110d".format(i)

    var maxKey = 0
    val keyMap = mutable.HashMap[Int, Int]()
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
      TimeUnit.SECONDS.sleep(1)
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
        case Some(expectKey) =>
          val expectedValue = genValue(expectKey)
          assertResult(expectedValue)(value.get)
          expectedEntries += entry(key, expectedValue)
        case None => assert(value.isEmpty)
    }
    checkIterator(expectedEntries.toList, storage.scan(Unbounded(), Unbounded()), false)
  }

  def checkCompactionRatio(storage: TinyLsm): Unit = {
    val snapshot = storage.inner.state.read(_.copy())
    val compactionOptions = storage.inner.options.compactionOptions
    val l0SstNum = snapshot.l0SsTables.length
    val levelSizes = snapshot.levels.map((_, sstIds) => compactionOptions match
      case l: LeveledCompactionOptions => sstIds.map(snapshot.ssTables(_).tableSize()).sum
      case _ => sstIds.length.toLong
    )
    val extraIterators = if (TS_ENABLED) 1 else 0
    val numIters = storage.scan(Unbounded(), Unbounded()).numActiveIterators()
    val numMemtables = snapshot.immutableMemTables.length + 1
    compactionOptions match
      case SimpleCompactionOptions(sizeRatioPercent, level0FileNumCompactionTrigger, maxLevels) =>
        assert(l0SstNum < level0FileNumCompactionTrigger)
        assert(levelSizes.length <= maxLevels)
        for (idx <- 1 until levelSizes.length) {
          val prevSize = levelSizes(idx - 1)
          val curSize = levelSizes(idx)
          if (prevSize != 0 && curSize != 0) {
            assert((curSize.toDouble / prevSize) >= sizeRatioPercent.toDouble / 100,
              s"L${snapshot.levels(idx - 1)._1}/L${snapshot.levels(idx)._1}, $curSize/$prevSize<$sizeRatioPercent%")
          }
        }
        assert(numIters <= (l0SstNum + numMemtables + maxLevels + extraIterators),
          s"we found $numIters iterators in your implementation, (l0SstNum=$l0SstNum, numMemtables=$numMemtables, maxLevels=$maxLevels) did you use concat iterators?")
      case TieredCompactionOptions(maxSizeAmplificationPercent, sizeRatio, minMergeWidth, numTiers) =>
        val sizeRatioTrigger = (100.0 + sizeRatio.toDouble) / 100.0
        assertResult(0)(l0SstNum)
        assert(levelSizes.length <= numTiers)
        var sumSize = levelSizes.head
        for (idx <- 1 until levelSizes.length) {
          val curSize = levelSizes(idx)
          if (levelSizes.length > minMergeWidth) {
            assert(sumSize.toDouble / curSize <= sizeRatioTrigger,
              s"violation of size ratio: sum(⬆️L${snapshot.levels(idx - 1)._1})/L${snapshot.levels(idx)._1}, $sumSize/$curSize>$sizeRatioTrigger")
          }
          if (idx + 1 == levelSizes.length) {
            assert(sumSize.toDouble / curSize <= maxSizeAmplificationPercent.toDouble / 100.0,
              s"violation of space amp: sum(⬆️L${snapshot.levels(idx - 1)._1})/L${snapshot.levels(idx)._1}, $sumSize/$curSize>$maxSizeAmplificationPercent%"
            )
          }
          sumSize += curSize
        }
        assert(numIters <= (numMemtables + numTiers + extraIterators),
          s"we found $numIters iterators in your implementation, (l0SstNum=$l0SstNum, numMemtables=$numMemtables, numTiers=$numTiers) did you use concat iterators?")
      case LeveledCompactionOptions(levelSizeMultiplier, level0FileNumCompactionTrigger, maxLevels, baseLevelSizeMb) =>
        assert(l0SstNum < level0FileNumCompactionTrigger)
        assert(levelSizes.length <= maxLevels)
        val lastLevelSize = levelSizes.last
        var multiplier = 1.0
        for (idx <- (1 until levelSizes.length).reverse) {
          multiplier *= levelSizeMultiplier.toDouble
          val curSize = levelSizes(idx - 1)
          assert((curSize.toDouble / lastLevelSize) <= (1.0 / multiplier + 0.5),
            s"L${snapshot.levels(idx - 1)._1}/L_max, $curSize/$lastLevelSize>>1.0/$multiplier"
          )
        }
        assert(
          numIters <= (l0SstNum + numMemtables + maxLevels + extraIterators),
          s"we found $numIters iterators in your implementation, (l0SstNum=$l0SstNum, numMemtables=$numMemtables, maxLevels=$maxLevels) did you use concat iterators?"
        );
      case _ =>
  }

  def dumpFilesInDir(path: File): Unit = {
    println("--- DIR DUMP ---")
    for (file <- path.listFiles()) {
      println(s"${file.getAbsolutePath}, size=${"%.3f".format(file.length() / 1024.0)}KB")
    }
  }
}
