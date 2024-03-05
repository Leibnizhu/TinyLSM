package io.github.leibnizhu.tinylsm

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.github.leibnizhu.tinylsm.compact.CompactionTask

import java.io.File

class Manifest(file: File) {

  def addRecord(): Unit = {

  }

  def recover(): Unit = {

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
trait ManifestRecord

case class ManifestFlush(flush: Int) extends ManifestRecord

case class ManifestNewMemtable(memtableId: Int) extends ManifestRecord

case class ManifestCompaction(compactionTask: CompactionTask, compactionNewSstId: List[Int]) 
  extends ManifestRecord
