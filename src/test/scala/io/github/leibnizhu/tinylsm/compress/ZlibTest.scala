package io.github.leibnizhu.tinylsm.compress

import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.util.zip.{Deflater, Inflater}

class ZlibTest extends AnyFunSuite {
  private val log = LoggerFactory.getLogger(this.getClass)

  private def valueOf(i: Int) = "value_" + "%02000d".format(i)

  // Java的Zlib实现（即java.util.zip.Deflater）并不直接支持动态字典。然而，你可以使用JNI（Java Native Interface）来调用底层的Zlib库，并在其中设置动态字典
  test("zlib_test") {
    val data = valueOf(9878).getBytes

    val deflater = new Deflater()
    deflater.reset()
    deflater.setInput(data)
    deflater.finish()
    val compressedData = new Array[Byte](data.length)
    val compressedSize = deflater.deflate(compressedData)
    val compressed = new Array[Byte](compressedSize)
    System.arraycopy(compressedData, 0, compressed, 0, compressedSize)
    deflater.end()
    // 可以看到，这个case下， 只要有两个key要压缩，那么字典+压缩后的两个key，就会小于两个原始key
    log.info("raw string size: {}, compressed size: {}",
      data.length, compressed.size)

    val inflater = new Inflater()
    inflater.setInput(compressed)
    val decompressedData = new Array[Byte](data.length)
    val decompressedSize = inflater.inflate(decompressedData)
    inflater.end()
    val decompressedStr = new String(decompressedData, 0, decompressedSize)
    log.info("decompressed: {}", decompressedStr)
    assertResult(valueOf(9878))(decompressedStr)
  }
}
