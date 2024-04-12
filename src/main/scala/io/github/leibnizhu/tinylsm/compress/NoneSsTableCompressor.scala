package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.MemTableValue
import io.github.leibnizhu.tinylsm.block.{Block, BlockMeta}
import io.github.leibnizhu.tinylsm.utils.ByteArrayWriter

import scala.collection.mutable.ArrayBuffer

class NoneSsTableCompressor extends SsTableCompressor {

  override def addDictSample(sample: MemTableValue): Unit = {}

  override def generateDict(): Array[Byte] = Array()

  override def compressSsTable(blockSize: Int, blockData: ByteArrayWriter,
                               blocks: ArrayBuffer[Block], meta: Array[BlockMeta]): (ByteArrayWriter, Array[BlockMeta]) =
    (blockData, meta)

  override def compress(origin: Array[Byte]): Array[Byte] = origin

  override def decompress(compressed: Array[Byte], originLength: Int): Array[Byte] = compressed

  override def close(): Unit = {}

  override val DICT_TYPE: Byte = NoneSsTableCompressor.DICT_TYPE
}

object NoneSsTableCompressor {
  val DICT_TYPE: Byte = 0
}