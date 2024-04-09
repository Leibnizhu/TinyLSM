package io.github.leibnizhu.tinylsm.utils

import java.io.{BufferedOutputStream, File, FileOutputStream, RandomAccessFile}

case class FileObject(file: Option[File], size: Long) {
  def read(offset: Long, length: Long): Array[Byte] = {
    if (file.isEmpty) {
      throw new IllegalArgumentException("FileObject cannot read file when file is not set")
    }
    if (length <= 0) {
      return Array[Byte]()
    }
    val accessFile = new RandomAccessFile(file.get, "r")
    accessFile.seek(offset)
    val buffer = new Array[Byte](length.intValue)
    val readBytes = accessFile.read(buffer)
    if (readBytes < length) {
      buffer.slice(0, readBytes)
    } else {
      buffer
    }
  }

}

object FileObject {
  def create(path: File, data: Array[Byte]): FileObject = {
    if (!path.exists()) {
      path.createNewFile()
    }
    val writer = new BufferedOutputStream(new FileOutputStream(path))
    writer.write(data)
    writer.close()
    FileObject(Some(path), data.length)
  }

  def open(path: File): FileObject = {
    if (!path.exists()) {
      throw new IllegalArgumentException("File does not exists: " + path.getAbsolutePath)
    }
    FileObject(Some(path), path.length())
  }
}