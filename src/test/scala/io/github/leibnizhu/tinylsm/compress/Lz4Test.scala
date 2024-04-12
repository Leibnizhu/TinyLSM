package io.github.leibnizhu.tinylsm.compress

import net.jpountz.lz4.LZ4Factory
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

class Lz4Test extends AnyFunSuite {
  private val log = LoggerFactory.getLogger(this.getClass)

  private def valueOf(i: Int) = "value_" + "%02000d".format(i)

  test("lz4_test") {
    val data = valueOf(43234).getBytes

    val factory = LZ4Factory.fastestInstance()
    val compressor = factory.fastCompressor()
    val compressed = compressor.compress(data)

    val decompressor = factory.fastDecompressor()
    val decompressed = decompressor.decompress(compressed, data.length)
    val decompressedStr = new String(decompressed)
    log.info("raw string size: {}, compressed size: {}", data.length, compressed.size)

    assertResult(valueOf(43234))(decompressedStr)
  }
}
