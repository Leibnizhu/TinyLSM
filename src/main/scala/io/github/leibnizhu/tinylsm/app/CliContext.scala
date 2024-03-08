package io.github.leibnizhu.tinylsm.app

import io.github.leibnizhu.tinylsm.utils.{Bound, Config}
import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageOptions}
import requests.{RequestFailedException, get}

import java.io.File
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.StringJoiner
import scala.jdk.CollectionConverters.*


class CliContext(playgroundMode: Boolean,
                 playgroundLsm: Option[LsmStorageInner],
                 debugMode: Boolean,
                 host: String,
                 port: Int) {

  def get(key: String): Unit = {
    if (playgroundMode) {
      val value = playgroundLsm.get.get(key)
      if (value.isDefined) {
        println(value.get)
      } else {
        println("> Key does not exists: " + key)
      }
    } else {
      try {
        val encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8)
        val r = requests.get(s"http://$host:$port/key/$encodedKey")
        println(r.text())
      } catch
        case e: RequestFailedException => if (e.response.statusCode == 404) {
          println(">>> Key does not exists: " + key)
        } else {
          println(">>> Server error: " + e.response.text())
        }
    }
  }

  def delete(key: String): Unit = {
    if (playgroundMode) {
      playgroundLsm.get.delete(key)
      println("Done")
    } else {
      val encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8)
      requests.delete(s"http://$host:$port/key/$encodedKey")
    }
  }

  def put(key: String, value: String): Unit = {
    if (playgroundMode) {
      playgroundLsm.get.put(key, value)
      println("Done")
    } else {
      val encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8)
      val encodedValue = URLEncoder.encode(value, StandardCharsets.UTF_8)
      requests.post(s"http://$host:$port/key/$encodedKey?value=$encodedValue")
    }
  }

  private val validBoundType = Set("unbounded", "excluded", "included")

  def scan(fromType: String, fromKey: String, toType: String, toKey: String): Unit = {
    if (!validBoundType.contains(fromType.toLowerCase)) {
      println("Invalid command, fromType must be one of: " + validBoundType)
    }
    if (!validBoundType.contains(toType.toLowerCase)) {
      println("Invalid command, toType must be one of: " + validBoundType)
    }

    if (playgroundMode) {
      val lower = Bound(fromType, fromKey)
      val upper = Bound(toType, toKey)
      val itr = playgroundLsm.get.scan(lower, upper)
      val sj = new StringJoiner("\n")
      println(itr.joinAllKeyValue(sj).toString)
    } else {
      val encodedFromKey = URLEncoder.encode(fromKey, StandardCharsets.UTF_8)
      val encodedToKey = URLEncoder.encode(toKey, StandardCharsets.UTF_8)
      val r = requests.get(s"http://$host:$port/scan?fromType=$fromType&fromKey=$encodedFromKey&toType=$toType&toKey=$encodedToKey")
      println(r.text())
    }
  }

  def flush(): Unit = {
    if(!debugMode) {
      println("flush command can only be used in debug mode!")
      return
    }
    if (playgroundMode) {
      playgroundLsm.get.forceFreezeMemTable()
      playgroundLsm.get.forceFlushNextImmutableMemTable()
    } else {
      requests.post(s"http://$host:$port/flush")
    }
  }
}

object CliContext {
  def apply(argMap: Map[String, Any]): CliContext = {
    val playgroundMode = argMap.getOrElse("playground", false).asInstanceOf[Boolean]
    val debugMode = argMap.getOrElse("debug", false).asInstanceOf[Boolean]
    val playgroundLsm = if (playgroundMode) {
      val tempDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "TinyLsmPlayground")
      if (tempDir.exists()) {
        tempDir.delete()
      }
      tempDir.mkdirs()
      println(s"Start a playground TinyLSM instance...")
      Some(LsmStorageInner(tempDir, LsmStorageOptions.defaultOption()))
    } else {
      None
    }
    new CliContext(
      playgroundMode,
      playgroundLsm,
      debugMode,
      argMap.getOrElse("host", "localhost").asInstanceOf[String],
      argMap.getOrElse("port", Config.Port.defaultVal.toInt).asInstanceOf[Int]
    )
  }
}