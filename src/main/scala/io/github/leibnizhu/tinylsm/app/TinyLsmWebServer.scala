package io.github.leibnizhu.tinylsm.app

import cask.model.Response
import io.github.leibnizhu.tinylsm.utils.{Bound, Config}
import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageOptions}

import java.io.File
import java.util.StringJoiner

object TinyLsmWebServer extends cask.MainRoutes {

  override def port: Int = Config.Port.getInt

  override def host: String = Config.Host.get()

  override def debugMode = false

  private val lsmOptions = LsmStorageOptions.fromConfig()
  private val dataDir = new File(Config.DataDir.get())
  private val storage = LsmStorageInner(dataDir, lsmOptions)

  @cask.get("/key/:key")
  def getByKey(key: String): Response[String] = {
    storage.get(key).map(cask.Response(_))
      .getOrElse(cask.Response("KeyNotExists", statusCode = 404))
  }

  @cask.delete("/key/:key")
  def deleteByKey(key: String): Response[String] = {
    storage.delete(key)
    cask.Response("", statusCode = 204)
  }

  @cask.post("/key/:key")
  def putValue(key: String, value: String): Response[String] = {
    storage.put(key, value)
    cask.Response("", statusCode = 204)
  }

  @cask.get("/scan")
  def scan(fromType: String, fromKey: String, toType: String, toKey: String): String = {
    val lower = Bound(fromType, fromKey)
    val upper = Bound(toType, toKey)
    val itr = storage.scan(lower, upper)
    val sj = new StringJoiner("\n")
    itr.joinAllKeyValue(sj).toString
  }

  @cask.post("/flush")
  def forceFlush(): Response[String] = {
    storage.forceFreezeMemTable()
    storage.forceFlushNextImmutableMemTable()
    cask.Response("", statusCode = 204)
  }

  Config.print()
  initialize()
}
