package io.github.leibnizhu.tinylsm.raft

import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter, Config}

import java.io.*
import java.util.concurrent.locks.ReentrantReadWriteLock

case class RaftPersistState(
                             currentTerm: Int,
                             votedFor: Option[Int],
                             log: Array[LogEntry],
                             snapshot: Array[Byte],
                             snapshotLastIndex: Int,
                             snapshotLastTerm: Int,
                           ) {
  // 计算每条命令是 命令本身长度+命令长度存储的4byte + 存储index的4byte + 存储term的4byte
  def logSize(): Int = log.map(_.command.length + 4 + 4 + 4).sum
}

sealed trait Persistor {

  val (unPersistLock, persistLock) = {
    val rwLock = ReentrantReadWriteLock()
    (rwLock.readLock(), rwLock.writeLock())
  }

  def persist(state: RaftPersistState): Unit = {
    val buf = new ByteArrayWriter()
    buf.putUint32(state.currentTerm).putUint32(state.votedFor.getOrElse(-1)).putUint32(state.log.length)
    state.log.foreach(logEntry => buf.putUint32(logEntry.term).putUint32(logEntry.index)
      .putUint32(logEntry.command.length).putBytes(logEntry.command))

    // 记录snapshot
    buf.putUint32(state.snapshotLastTerm).putUint32(state.snapshotLastIndex)
      .putUint32(state.snapshot.length).putBytes(state.snapshot)
    doPersist(buf.toArray)
  }

  def readPersist(): Option[RaftPersistState] = {
    val bytes = doReadPersist()
    if (bytes == null) {
      return None
    }
    val buf = new ByteArrayReader(bytes)
    if (buf.remaining <= 0) {
      return None
    }
    val currentTerm = buf.readUint32()
    val votedFor = {
      val v = buf.readUint32()
      if (v == -1) None else Some(v)
    }
    // 读日志
    val logLength = buf.readUint32()
    val log = new Array[LogEntry](logLength)
    for (i <- 0 until logLength) {
      val logTerm = buf.readUint32()
      val logIndex = buf.readUint32()
      val commandLength = buf.readUint32()
      val command = buf.readBytes(commandLength)
      log(i) = LogEntry(logTerm, logTerm, command)
    }

    val snapshotLastTerm = buf.readUint32()
    val snapshotLastIndex = buf.readUint32()
    val snapshotLen = buf.readUint32()
    val snapshot = if (snapshotLen > 0) {
      buf.readBytes(snapshotLen)
    } else Array[Byte]()
    Some(RaftPersistState(
      currentTerm = currentTerm,
      votedFor = votedFor,
      log = log,
      snapshot = snapshot,
      snapshotLastIndex = snapshotLastIndex,
      snapshotLastTerm = snapshotLastTerm
    ))
  }

  def doPersist(data: Array[Byte]): Unit

  def doReadPersist(): Array[Byte]

  def size(): Int
}

object PersistorFactory {
  val MEMORY = "memory"
  val FILE = "file"

  def byConfig(nodeIndex: Int): Persistor = Config.RaftPersistorType.get().toLowerCase match
    case MEMORY => MemoryPersistor(nodeIndex)
    case FILE => FilePersistor(new File(Config.DataDir.get() + File.separator + "raft-" + nodeIndex + ".state"))
}

case class MemoryPersistor(nodeIdx: Int) extends Persistor {
  private var data: Array[Byte] = _

  override def doPersist(data: Array[Byte]): Unit = try {
    persistLock.lock()
    this.data = data
  } finally {
    persistLock.unlock()
  }

  override def doReadPersist(): Array[Byte] = try {
    unPersistLock.lock()
    this.data
  } finally {
    unPersistLock.unlock()
  }

  override def size(): Int = data.length
}

case class FilePersistor(file: File) extends Persistor {

  if (!file.getParentFile.exists()) {
    file.getParentFile.mkdirs()
  }
  if (!file.exists()) {
    file.createNewFile()
  }

  override def doPersist(data: Array[Byte]): Unit = try {
    persistLock.lock()
    val writer = new BufferedOutputStream(new FileOutputStream(file, true))
    writer.write(data)
    writer.close()
  } finally {
    persistLock.unlock()
  }


  override def doReadPersist(): Array[Byte] = try {
    unPersistLock.lock()
    new BufferedInputStream(FileInputStream(file)).readAllBytes()
  } finally {
    unPersistLock.unlock()
  }

  override def size(): Int = file.length().toInt
}