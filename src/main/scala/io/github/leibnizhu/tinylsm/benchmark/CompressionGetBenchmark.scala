package io.github.leibnizhu.tinylsm.benchmark

import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.compress.CompressorOptions
import io.github.leibnizhu.tinylsm.{LsmStorageOptions, TinyLsm}
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.io.File

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
class CompressionGetBenchmark {
  private var zstdStorageDir: File = _
  private var zstdStorage: TinyLsm = _
  private var zlibStorageDir: File = _
  private var zlibStorage: TinyLsm = _

  private def keyOf(i: Int) = "key_" + "%02000d".format(i)

  private def valueOf(i: Int) = "value_" + "%02000d".format(i)

  @Setup(Level.Trial)
  def init(): Unit = {
    zstdStorageDir = new File(List(System.getProperty("java.io.tmpdir"), "LsmBenchmark-Zstd", System.currentTimeMillis().toString)
      .mkString(File.separator))
    zstdStorageDir.mkdirs()
    val compactOption = CompactionOptions.LeveledCompactionOptions(2, 2, 4, 20)
    val zstdOptions = LsmStorageOptions.defaultOption().copy(numMemTableLimit = 2,
      compactionOptions = compactOption, compressorOptions = CompressorOptions.Zstd())
    zstdStorage = TinyLsm(zstdStorageDir, zstdOptions)
    for (i <- 1 until 10000) {
      zstdStorage.put(keyOf(i), valueOf(i))
    }

    zlibStorageDir = new File(List(System.getProperty("java.io.tmpdir"), "LsmBenchmark-Zlib", System.currentTimeMillis().toString)
      .mkString(File.separator))
    zlibStorageDir.mkdirs()
    val zlibOptions = LsmStorageOptions.defaultOption().copy(numMemTableLimit = 2,
      compactionOptions = compactOption, compressorOptions = CompressorOptions.Zlib())
    zlibStorage = TinyLsm(zlibStorageDir, zlibOptions)
    for (i <- 1 until 10000) {
      zlibStorage.put(keyOf(i), valueOf(i))
    }
  }

  @Benchmark
  def get10kZstd(blackHole: Blackhole): String = {
    for (i <- 1 until 10000) {
      zstdStorage.get(keyOf(i))
    }
    val value = zstdStorage.get(keyOf(1)).get
    blackHole.consume(value)
    value
  }

  @Benchmark
  def get10kZlib(blackHole: Blackhole): String = {
    for (i <- 1 until 10000) {
      zlibStorage.get(keyOf(i))
    }
    val value = zlibStorage.get(keyOf(1)).get
    blackHole.consume(value)
    value
  }

  @TearDown(Level.Trial)
  def close(): Unit = {
    zstdStorage.close()
    zstdStorageDir.listFiles().foreach(_.delete())
    zstdStorageDir.delete()
    zlibStorage.close()
    zlibStorageDir.listFiles().foreach(_.delete())
    zlibStorageDir.delete()
  }
}
