package io.github.leibnizhu.tinylsm

import cask.model.Response

import java.io.File

object TinyLsmWebServer extends cask.MainRoutes {

  override def port: Int = Config.Port.getInt()

  override def host: String = Config.Host.get()

  private val lsmOptions = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
  private val tempDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "TinyLsm")
  private val storage = LsmStorageInner(tempDir, lsmOptions)

  @cask.get("/key/:key")
  def getByKey(key: String): Response[String] = {
    storage.get(key).map(cask.Response(_))
      .getOrElse(cask.Response("KeyNotExists", statusCode = 404))
  }

  @cask.delete("/key/:key")
  def deleteByKey(key: String): Unit = {
    storage.delete(key)
  }

  @cask.post("/key/:key")
  def putValue(key: String, value: String): Unit = {
    storage.put(key, value)
  }

  initialize()
}

enum Config(private val envName: String, private val defaultVal: String) {

  private val sysPropName = toSysPropertyName(envName)

  case Port extends Config("TINY_LSM_PORT", "9527")
  case Host extends Config("TINY_LSM_LISTEN", "0.0.0.0")

  private def toSysPropertyName(str: String): String = str.replace("_", ".").toLowerCase

  def get(): String = {
    val sysProp = System.getProperty(sysPropName)
    if (sysProp != null && sysProp.nonEmpty) {
      return sysProp
    }
    val envProp = System.getenv(envName)
    if (envProp != null && envProp.nonEmpty) {
      return sysProp
    }
    defaultVal
  }

  def getInt(): Int = get().toInt
}