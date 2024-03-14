package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter}

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.hashing.MurmurHash3

/**
 * WAL 格式
 * | key_len(2B) | key | timestamp(8B) | value_len(2B) | value | checksum(4B)
 *
 * @param walFile WAL文件对象
 */
case class WriteAheadLog(walFile: File) {
  private lazy val writer = new BufferedOutputStream(FileOutputStream(walFile))
  private val (readLock, writeLock) = {
    val rwLock = ReentrantReadWriteLock()
    (rwLock.readLock(), rwLock.writeLock())
  }

  def recover(toMap: java.util.Map[MemTableKey, MemTableValue]): WriteAheadLog = {
    try {
      readLock.lock()
      val buffer = new ByteArrayReader(FileInputStream(walFile).readAllBytes())
      while (buffer.remaining > 0) {
        val startOffset = buffer.curPos
        val keyLen = buffer.readUint16()
        val key = buffer.readBytes(keyLen)
        val ts = buffer.readUint64()
        val valueLen = buffer.readUint16()
        val value = buffer.readBytes(valueLen)
        val checksum = MurmurHash3.bytesHash(buffer.bytes.slice(startOffset, buffer.curPos))
        val readHash = buffer.readUint32()
        if (checksum != readHash) {
          throw new IllegalStateException("WAL checksum mismatched")
        }
        toMap.put(MemTableKey(key, ts), value)
      }
      this
    } finally {
      readLock.unlock()
    }
  }

  def put(mKey: MemTableKey, value: Array[Byte]): Unit = {
    try {
      writeLock.lock()
      val buffer = new ByteArrayWriter(mKey.rawLength + value.length + SIZE_OF_U16 * 2 + SIZE_OF_INT)
      buffer.putUint16(mKey.length).putKey(mKey).putUint16(value.length).putBytes(value)
      val hash = MurmurHash3.bytesHash(buffer.toArray)
      buffer.putUint32(hash)
      writer.write(buffer.toArray)
    } finally {
      writeLock.unlock()
    }
  }

  def sync(): Unit = writer.flush()
}
