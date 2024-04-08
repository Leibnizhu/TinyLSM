package io.github.leibnizhu.tinylsm.block

import com.github.luben.zstd.{Zstd, ZstdDictCompress, ZstdDictDecompress, ZstdDictTrainer}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets

class ZstdDictTest extends AnyFunSuite {
  private val log = LoggerFactory.getLogger(this.getClass)

  private def valueOf(i: Int) = "value_" + "%02000d".format(i)

  test("dictUse") {
    // 采样训练字典
    val trainer = new ZstdDictTrainer(1024 * 1024, 16 * 1024)
    for (i <- 1 until 10000 by 100) {
      trainer.addSample(valueOf(i).getBytes)
    }
    val dictionary = trainer.trainSamples

    // 压缩
    val raw = valueOf(9878)
    val zstdDictCompress = new ZstdDictCompress(dictionary, Zstd.defaultCompressionLevel)
    val compressed = Zstd.compress(raw.getBytes, zstdDictCompress)
    val jsonFullLength = raw.getBytes.length

    // 可以看到，这个case下， 只要有两个key要压缩，那么字典+压缩后的两个key，就会小于两个原始key
    log.info("dictionary size: {}, raw string size: {}, compressed size: {}",
      dictionary.size, jsonFullLength, compressed.size)

    // 解压
    val zstdDictDecompress = new ZstdDictDecompress(dictionary)
    val decompressed = Zstd.decompress(compressed, zstdDictDecompress, jsonFullLength)
    val decoded = new String(decompressed, StandardCharsets.UTF_8)
    assertResult(raw)(decoded)
  }
}
