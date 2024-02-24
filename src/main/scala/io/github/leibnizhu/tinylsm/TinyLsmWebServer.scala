package io.github.leibnizhu.tinylsm

import cask.model.Response

import java.io.File

object TinyLsmWebServer extends cask.MainRoutes {

  override def port: Int = Config.Port.getInt()

  override def host: String = "0.0.0.0"

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

  private val sysPropName = underlineToHump(envName)

  case Port extends Config("TINY_LSM_PORT", "9527")

  def underlineToHump(str: String): String = {
    var spStr: Array[String] = str.split("_")
    //循环这个数组
    var result = ""
    var index = 0
    for (i <- 0 until str.length) {
      if (str.charAt(i) == '_') {
        index = 1 + i
      } else {
        if (i == index && i != 0) {
          result += str.charAt(i).toUpper
        } else {
          result += str.charAt(i)
        }
      }
    }
    result
  }

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