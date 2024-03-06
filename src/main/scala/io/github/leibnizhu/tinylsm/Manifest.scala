package io.github.leibnizhu.tinylsm

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.leibnizhu.tinylsm.compact.CompactionTask
import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter}

import java.io.*
import scala.collection.mutable.ListBuffer

class Manifest(file: File) {
  private val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()

  def addRecord(record: ManifestRecord): Unit = {
    if (!file.exists()) {
      file.createNewFile()
    }
    val buffer = new ByteArrayWriter()
    val jsonBytes = mapper.writeValueAsBytes(record)
    buffer.putUint32(jsonBytes.length)
    buffer.putBytes(jsonBytes)
    buffer.putUint32(byteArrayHash(jsonBytes))
    val writer = new BufferedOutputStream(new FileOutputStream(file, true))
    writer.write(buffer.toArray)
    writer.close()
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
))
sealed trait ManifestRecord

case class ManifestFlush(sstId: Int) extends ManifestRecord

case class ManifestNewMemtable(memtableId: Int) extends ManifestRecord

case class ManifestCompaction(task: CompactionTask, output: List[Int])
  extends ManifestRecord
