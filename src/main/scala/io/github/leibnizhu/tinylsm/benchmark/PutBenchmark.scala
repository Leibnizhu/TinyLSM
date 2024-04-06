package io.github.leibnizhu.tinylsm.benchmark

import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.{LsmStorageOptions, TinyLsm}
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import org.slf4j.{Logger, LoggerFactory}

import java.io.File

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
class PutBenchmark {
  private var storageDir: File = _
  private var storage: TinyLsm = _

  @Setup(Level.Trial)
  def init(): Unit = {
    val tempDirPath = List(System.getProperty("java.io.tmpdir"), "LsmBenchmark", System.currentTimeMillis().toString)
      .mkString(File.separator)
    storageDir = new File(tempDirPath)
    storageDir.mkdirs()
    val compactOption = CompactionOptions.LeveledCompactionOptions(2, 2, 4, 20)
    val options = LsmStorageOptions(4096, 1 << 20, 1 << 20, 2, compactOption, false, false)
    storage = TinyLsm(storageDir, options)
  }

  @Benchmark
  def put10k(blackHole: Blackhole): Long = {
    val ts = System.currentTimeMillis()
    for (_ <- 1 until 10000) {
      storage.put("key-" + ts, "value-" + ts)
    }
    blackHole.consume(ts)
    ts
  }

  @TearDown(Level.Trial)
  def close(): Unit = {
    storage.close()
    storageDir.listFiles().foreach(_.delete())
    storageDir.delete()
  }
}
