package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.MemTableValue

object NoneSsTableCompressor extends SsTableCompressor {

  override def addDictSample(sample: MemTableValue): Unit = {}

  override def generateDict(): Array[Byte] = Array()

  override def compress(origin: Array[Byte]): Array[Byte] = origin

  override def decompress(compressed: Array[Byte], originLength: Int): Array[Byte] = compressed

  override def close(): Unit = {}

  override val DICT_TYPE: Byte = 0
}