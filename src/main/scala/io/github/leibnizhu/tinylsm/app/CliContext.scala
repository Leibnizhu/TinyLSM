package io.github.leibnizhu.tinylsm.app

import io.github.leibnizhu.tinylsm.mvcc.Transaction
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
  // for playground 模式
  private var currentTxn: Option[Transaction] = None
  // for 连接服务器模式，非 playground
  private var currentTxnId: Option[Int] = None

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    rollbackCurrentTxn()
  }))

  def get(key: String): Unit =
    if (playgroundMode) {
      val value = currentTxn match
        case Some(txn) => txn.get(key)
        case None => playgroundLsm.get.get(key)
      if (value.isDefined) {
        println(value.get)
      } else {
        println(">>> Key does not exists: " + key)
      }
    } else {
      try {
        val encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8)
        val tidParam = currentTxnId.map(tid => s"?tid=$tid").getOrElse("")
        val r = requests.get(s"http://$host:$port/key/$encodedKey$tidParam")
        println(r.text())
      } catch
        case e: RequestFailedException => if (e.response.statusCode == 404) {
          println(">>> Key does not exists: " + key)
        } else {
          println(">>> Server error: " + e.response.text())
        }
    }

  def delete(key: String): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) => txn.delete(key)
        case None => playgroundLsm.get.delete(key)
      println("Done")
    } else {
      val encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8)
      val tidParam = currentTxnId.map(tid => s"?tid=$tid").getOrElse("")
      requests.delete(s"http://$host:$port/key/$encodedKey$tidParam")
    }

  def put(key: String, value: String): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) => txn.put(key, value)
        case None => playgroundLsm.get.put(key, value)
      println("Done")
    } else {
      val encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8)
      val encodedValue = URLEncoder.encode(value, StandardCharsets.UTF_8)
      val tidParam = currentTxnId.map(tid => s"&tid=$tid").getOrElse("")
      requests.post(s"http://$host:$port/key/$encodedKey?value=$encodedValue$tidParam")
    }

  private val validBoundType = Set("unbounded", "excluded", "included")

  def scan(fromType: String, fromKey: String, toType: String, toKey: String): Unit = {
    if (!validBoundType.contains(fromType.toLowerCase)) {
      println(">>> Invalid command, fromType must be one of: " + validBoundType)
    }
    if (!validBoundType.contains(toType.toLowerCase)) {
      println(">>> Invalid command, toType must be one of: " + validBoundType)
    }

    if (playgroundMode) {
      val lower = Bound(fromType, fromKey)
      val upper = Bound(toType, toKey)
      val itr = currentTxn match
        case Some(txn) => txn.scan(lower, upper)
        case None => playgroundLsm.get.scan(lower, upper)
      val sj = new StringJoiner("\n")
      println(itr.joinAllKeyValue(sj).toString)
    } else {
      val encodedFromKey = URLEncoder.encode(fromKey, StandardCharsets.UTF_8)
      val encodedToKey = URLEncoder.encode(toKey, StandardCharsets.UTF_8)
      val tidParam = currentTxnId.map(tid => s"&tid=$tid").getOrElse("")
      val r = requests.get(s"http://$host:$port/scan?fromType=$fromType&fromKey=$encodedFromKey&toType=$toType&toKey=$encodedToKey$tidParam")
      println(r.text())
    }
  }

  def flush(): Unit = {
    if (!debugMode) {
      println(">>> flush command can only be used in debug mode!")
      return
    }
    if (playgroundMode) {
      playgroundLsm.get.forceFreezeMemTable()
      playgroundLsm.get.forceFlushNextImmutableMemTable()
    } else {
      val r = requests.post(s"http://$host:$port/sys/flush")
      println(">>> " + r.text())
    }
  }

  def status(): Unit = {
    if (!debugMode) {
      println(">>> status command can only be used in debug mode!")
      return
    }
    if (playgroundMode) {
      playgroundLsm.get.dumpState()
    } else {
      val r = requests.post(s"http://$host:$port/sys/state")
      println(r.text())
    }
  }

  def newTxn(): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) => println(s">>> Already start a Transaction, ID= ${txn.tid}")
        case None =>
          currentTxn = Some(playgroundLsm.get.newTxn())
          println(s"Start a new Transaction, ID: ${currentTxn.get.tid}")
    } else {
      currentTxnId match
        case Some(tid) => println(s">>> Already start a Transaction, ID= $tid")
        case None =>
          val r = requests.post(s"http://$host:$port/txn")
          if (r.statusCode / 100 == 2) {
            currentTxnId = Some(r.text().toInt)
            println(s">>> Start a new Transaction, ID: ${currentTxnId.get}")
          } else {
            println(">>> " + r.text())
          }
    }

  def commitCurrentTxn(): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) =>
          txn.commit()
          currentTxn = None
          println(s">>> Committed Transaction, ID: ${txn.tid}")
        case None => println(">>> No active Transaction!")
    } else {
      currentTxnId match
        case Some(tid) =>
          val r = requests.post(s"http://$host:$port/txn/$tid/commit")
          if (r.statusCode / 100 == 2) {
            currentTxnId = None
            println(s">>> Committed Transaction, ID: $tid")
          } else {
            println(">>> " + r.text())
          }
        case None => println(">>> No active Transaction!")
    }

  def rollbackCurrentTxn(): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) =>
          txn.rollback()
          currentTxn = None
          println(s">>> Rollback Transaction, ID: ${txn.tid}")
        case None => println(">>> No active Transaction!")
    } else {
      currentTxnId match
        case Some(tid) =>
          val r = requests.post(s"http://$host:$port/txn/$tid/rollback")
          if (r.statusCode / 100 == 2) {
            currentTxnId = None
            println(s">>> Rollback Transaction, ID: $tid")
          } else {
            println(">>> " + r.text())
          }
        case None => println(">>> No active Transaction!")
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
      argMap.getOrElse("port", Config.HttpPort.defaultVal.toInt).asInstanceOf[Int]
    )
  }
}