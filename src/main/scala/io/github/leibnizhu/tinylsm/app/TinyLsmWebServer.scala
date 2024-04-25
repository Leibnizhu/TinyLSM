package io.github.leibnizhu.tinylsm.app

import cask.model.Response
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.{Bound, Config}
import io.github.leibnizhu.tinylsm.{LsmStorageOptions, TinyLsm}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.StringJoiner
import java.util.concurrent.ConcurrentHashMap

object TinyLsmWebServer extends cask.MainRoutes {

  override def port: Int = Config.HttpPort.getInt

  override def host: String = Config.Host.get()

  override def debugMode = false

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val lsmOptions = LsmStorageOptions.fromConfig()
  private val dataDir = new File(Config.DataDir.get())
  private val storage = TinyLsm(dataDir, lsmOptions)
  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    logger.info("Start to close TinyLSM...")
    storage.close()
    logger.info("Finish closing TinyLSM...")
  }))
  private val transactions = new ConcurrentHashMap[Int, Transaction]()

  @cask.get("/key/:key")
  def getByKey(key: String, tid: Option[Int] = None): Response[String] = tid match
    case None =>
      storage.get(key).map(cask.Response(_))
        .getOrElse(cask.Response("KeyNotExists", statusCode = 404))
    case Some(tid) =>
      val transaction = transactions.get(tid)
      if (transaction == null) {
        cask.Response(s"Transaction with ID=$tid is not exists", statusCode = 404)
      } else if (transaction.isCommited) {
        cask.Response(s"Transaction with ID=$tid is already committed or rollback", statusCode = 400)
      } else {
        transaction.get(key).map(cask.Response(_))
          .getOrElse(cask.Response("KeyNotExists", statusCode = 404))
      }

  @cask.delete("/key/:key")
  def deleteByKey(key: String, tid: Option[Int] = None): Response[String] = tid match
    case None =>
      storage.delete(key)
      cask.Response("", statusCode = 204)
    case Some(tid) =>
      val transaction = transactions.get(tid)
      if (transaction == null) {
        cask.Response(s"Transaction with ID=$tid is not exists", statusCode = 404)
      } else if (transaction.isCommited) {
        cask.Response(s"Transaction with ID=$tid is already committed or rollback", statusCode = 400)
      } else {
        transaction.delete(key)
        cask.Response("", statusCode = 204)
      }

  @cask.post("/key/:key")
  def putValue(key: String, value: String, tid: Option[Int] = None): Response[String] = tid match
    case None =>
      storage.put(key, value)
      cask.Response("", statusCode = 204)
    case Some(tid) =>
      val transaction = transactions.get(tid)
      if (transaction == null) {
        cask.Response(s"Transaction with ID=$tid is not exists", statusCode = 404)
      } else if (transaction.isCommited) {
        cask.Response(s"Transaction with ID=$tid is already committed or rollback", statusCode = 400)
      } else {
        transaction.put(key, value)
        cask.Response("", statusCode = 204)
      }

  @cask.get("/scan")
  def scan(fromType: String, fromKey: String, toType: String, toKey: String,
           tid: Option[Int] = None): Response[String] = {
    val lower = Bound(fromType, fromKey)
    val upper = Bound(toType, toKey)
    tid match
      case None =>
        val itr = storage.scan(lower, upper)
        val sj = new StringJoiner("\n")
        cask.Response(itr.joinAllKeyValue(sj).toString, statusCode = 200)
      case Some(tid) =>
        val transaction = transactions.get(tid)
        if (transaction == null) {
          cask.Response(s"Transaction with ID=$tid is not exists", statusCode = 404)
        } else if (transaction.isCommited) {
          cask.Response(s"Transaction with ID=$tid is already committed or rollback", statusCode = 400)
        } else {
          val itr = transaction.scan(lower, upper)
          val sj = new StringJoiner("\n")
          cask.Response(itr.joinAllKeyValue(sj).toString, statusCode = 200)
        }
  }

  @cask.post("/sys/flush")
  def forceFlush(): Response[String] = {
    storage.forceFlush()
    cask.Response("Flush success", statusCode = 204)
  }

  @cask.post("/sys/state")
  def dumpState(): Response[String] = {
    val status = storage.inner.dumpState()
    cask.Response(status, statusCode = 200)
  }

  @cask.post("/txn")
  def newTransaction(): Response[Int] = {
    val txn = storage.newTxn()
    transactions.put(txn.tid, txn)
    cask.Response(txn.tid)
  }

  @cask.post("/txn/:tid/commit")
  def commitTransaction(tid: Int): Response[String] = {
    val transaction = transactions.remove(tid)
    if (transaction == null) {
      cask.Response(s"Transaction with ID=$tid is not exists", statusCode = 404)
    } else if (transaction.isCommited) {
      cask.Response(s"Transaction with ID=$tid is already committed or rollback", statusCode = 400)
    } else
      try {
        transaction.commit()
        cask.Response(s"Commit Transaction success", statusCode = 204)
      } catch
        case e: IllegalStateException => cask.Response(e.getMessage, statusCode = 409)
        case e: Exception => cask.Response("TinyLSM Server error", statusCode = 500)
  }

  @cask.post("/txn/:tid/rollback")
  def rollbackTransaction(tid: Int): Response[String] = {
    val transaction = transactions.remove(tid)
    if (transaction == null) {
      cask.Response(s"Transaction with ID=$tid is not exists", statusCode = 404)
    } else if (transaction.isCommited) {
      cask.Response(s"Transaction with ID=$tid is already committed or rollback", statusCode = 400)
    } else
      try {
        transaction.rollback()
        cask.Response(s"Rollback Transaction(ID=$tid) success", statusCode = 204)
      } catch
        case e: Exception => cask.Response("TinyLSM Server error", statusCode = 500)
  }

  Config.print()
  initialize()
}
