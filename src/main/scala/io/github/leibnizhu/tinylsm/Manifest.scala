package io.github.leibnizhu.tinylsm

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.leibnizhu.tinylsm.compact.CompactionTask
import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter}
import org.slf4j.LoggerFactory

import java.io.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ListBuffer

/**
 * 定时任务，如果Manifest体积超过一定阈值，则重写Manifest内容，只记录当前快照。
 * 要上锁
 *
 * @param file       Manifest 文件
 * @param targetSize 目标体积，默认1KB for测试，到达这个体积时合并Manifest，缩小体积
 */
class Manifest(file: File, targetSize: Int = 1024) {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
  private val (recordLock, compactLock) = {
    val rwLock = ReentrantReadWriteLock()
    (rwLock.readLock(), rwLock.writeLock())
  }

  if (!file.exists()) {
    file.createNewFile()
  }

  def addRecord(record: ManifestRecord): Unit = {
    try {
      recordLock.lock()
      val buffer = new ByteArrayWriter()
      val jsonBytes = mapper.writeValueAsBytes(record)
      buffer.putUint32(jsonBytes.length)
      buffer.putBytes(jsonBytes)
      buffer.putUint32(byteArrayHash(jsonBytes))
      val writer = new BufferedOutputStream(new FileOutputStream(file, true))
      writer.write(buffer.toArray)
      writer.close()
    } finally {
      recordLock.unlock()
    }
  }

  def tryCompact(snapshot: LsmStorageState): Unit = {
    val curSize = file.length()
    if (curSize > targetSize) {
      try {
        compactLock.lock()
        if (file.exists()) {
          val backupFile = new File(file.getAbsolutePath + ".backup." + System.currentTimeMillis())
          file.renameTo(backupFile)
          backupFile.deleteOnExit()
        }
        file.createNewFile()
        val record = ManifestSnapshot(snapshot.memTable.id, snapshot.immutableMemTables.map(_.id), snapshot.l0SsTables, snapshot.levels)
        addRecord(record)
        log.info("Manifest size before and after compact: {} => {}", curSize, file.length())

      } finally {
        compactLock.unlock()
      }
    }
  }

  def recover(): List[ManifestRecord] = {
    val buffer = new ByteArrayReader(FileInputStream(file).readAllBytes())
    val records = new ListBuffer[ManifestRecord]()
    while (buffer.remaining > 0) {
      val len = buffer.readUint32()
      val recordBytes = buffer.readBytes(len)
      val checksum = buffer.readUint32()
      if (checksum != byteArrayHash(recordBytes)) {
        throw new IllegalStateException("MANIFEST checksum mismatched!")
      }
      val record = mapper.readValue(recordBytes, classOf[ManifestRecord])
      records += record
    }
    records.toList
  }
}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[ManifestFlush], name = "flush"),
  new Type(value = classOf[ManifestNewMemtable], name = "memtable"),
  new Type(value = classOf[ManifestCompaction], name = "compaction"),
  new Type(value = classOf[ManifestSnapshot], name = "snapshot"),
))
sealed trait ManifestRecord

case class ManifestFlush(sstId: Int) extends ManifestRecord

case class ManifestNewMemtable(id: Int) extends ManifestRecord

case class ManifestCompaction(task: CompactionTask, output: List[Int])
  extends ManifestRecord

case class ManifestSnapshot(curMt: Int, frozenMt: List[Int], l0: List[Int], levels: List[(Int, List[Int])])
  extends ManifestRecord
