package io.github.leibnizhu.tinylsm

import java.io.File

case class WriteAheadLog(walPath: File) {
  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    //TODO
  }
}
