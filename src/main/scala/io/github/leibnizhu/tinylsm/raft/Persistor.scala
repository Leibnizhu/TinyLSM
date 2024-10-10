package io.github.leibnizhu.tinylsm.raft

import io.github.leibnizhu.tinylsm.utils.Config

import java.io.*
import java.util.concurrent.locks.ReentrantReadWriteLock

sealed trait Persistor {

  val (unPersistLock, persistLock) = {
    val rwLock = ReentrantReadWriteLock()
    (rwLock.readLock(), rwLock.writeLock())
  }

  def persist(data: Array[Byte]): Unit

  def readPersist(): Array[Byte]
}

object PersistorFactory {
  val MEMORY = "memory"
  val FILE = "file"

  def byConfig(nodeIndex: Int): Persistor = Config.RaftPersistorType.get().toLowerCase match
    case MEMORY => MemoryPersistor(nodeIndex)
    case FILE => new FilePersistor(new File(Config.DataDir.get() + File.separator + "raft-" + nodeIndex + ".state"))
}

case class MemoryPersistor(nodeIdx: Int) extends Persistor {
  private var data: Array[Byte] = _

  override def persist(data: Array[Byte]): Unit = try {
    persistLock.lock()
    this.data = data
  } finally {
    persistLock.unlock()
  }

  override def readPersist(): Array[Byte] = try {
    unPersistLock.lock()
    this.data
  } finally {
    unPersistLock.unlock()
  }
}

class FilePersistor(val file: File) extends Persistor {

  if (!file.getParentFile.exists()) {
    file.getParentFile.mkdirs()
  }
  if (!file.exists()) {
    file.createNewFile()
  }

  override def persist(data: Array[Byte]): Unit = try {
    persistLock.lock()
    val writer = new BufferedOutputStream(new FileOutputStream(file, true))
    writer.write(data)
    writer.close()
  } finally {
    persistLock.unlock()
  }


  override def readPersist(): Array[Byte] = try {
    unPersistLock.lock()
    new BufferedInputStream(FileInputStream(file)).readAllBytes()
  } finally {
    unPersistLock.unlock()
  }
}