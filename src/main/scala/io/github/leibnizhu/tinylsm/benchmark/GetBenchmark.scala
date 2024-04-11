package io.github.leibnizhu.tinylsm.benchmark

import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.compress.CompressorOptions
import io.github.leibnizhu.tinylsm.{LsmStorageOptions, TinyLsm}
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.io.File

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
class GetBenchmark {
  private var storageDir: File = _
  private var storage: TinyLsm = _

  private def keyOf(i: Int) = "key_" + "%02000d".format(i)

  private def valueOf(i: Int) = "value_" + "%02000d".format(i)

  @Setup(Level.Trial)
  def init(): Unit = {
    val tempDirPath = List(System.getProperty("java.io.tmpdir"), "LsmBenchmark", System.currentTimeMillis().toString)
      .mkString(File.separator)
    storageDir = new File(tempDirPath)
    storageDir.mkdirs()
    val compactOption = CompactionOptions.LeveledCompactionOptions(2, 2, 4, 20)
    val options = LsmStorageOptions(4096, 1 << 20, 1 << 20, 2, compactOption, CompressorOptions.None, false, false)
    storage = TinyLsm(storageDir, options)
    for (i <- 1 until 10000) {
      storage.put(keyOf(i), valueOf(i))
    }
  }

  @Benchmark
  def get10k(blackHole: Blackhole): String = {
    for (i <- 1 until 10000) {
      storage.get(keyOf(i))
    }
    val value = storage.get(keyOf(1)).get
    blackHole.consume(value)
    value
  }

  @TearDown(Level.Trial)
  def close(): Unit = {
    storage.close()
    storageDir.listFiles().foreach(_.delete())
    storageDir.delete()
  }
}
