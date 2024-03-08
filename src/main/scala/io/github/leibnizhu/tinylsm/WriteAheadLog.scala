package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter}

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}
import scala.util.hashing.MurmurHash3

/**
 * WAL 格式
 * | key_len | key | value_len | value |
 *
 * @param walFile WAL文件对象
 */
case class WriteAheadLog(walFile: File) {
  private lazy val writer = new BufferedOutputStream(FileOutputStream(walFile))
  private val (readLock, writeLock) = {
    val rwLock = ReentrantReadWriteLock()
    (rwLock.readLock(), rwLock.writeLock())
  }

  def recover(toMap: java.util.Map[ByteArrayKey, MemTableValue]): WriteAheadLog = {
    try {
      readLock.lock()
      val buffer = new ByteArrayReader(FileInputStream(walFile).readAllBytes())
      while (buffer.remaining > 0) {
        val startOffset = buffer.curPos
        val keyLen = buffer.readUint16()
        val key = buffer.readBytes(keyLen)
        val valueLen = buffer.readUint16()
        val value = buffer.readBytes(valueLen)
        val checksum = MurmurHash3.bytesHash(buffer.bytes.slice(startOffset, startOffset + keyLen + valueLen + SIZE_OF_U16 * 2))
        val readHash = buffer.readUint32()
        if (checksum != readHash) {
          throw new IllegalStateException("WAL checksum mismatched")
        }
        toMap.put(ByteArrayKey(key), value)
      }
      this
    } finally {
      readLock.unlock()
    }
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    try {
      writeLock.lock()
      val buffer = new ByteArrayWriter(key.length + value.length + SIZE_OF_U16 * 4)
      buffer.putUint16(key.length).putBytes(key).putUint16(value.length).putBytes(value)
      val hash = MurmurHash3.bytesHash(buffer.toArray)
      buffer.putUint32(hash)
      writer.write(buffer.toArray)
    } finally {
      writeLock.unlock()
    }
  }

  def sync(): Unit = writer.flush()
}
