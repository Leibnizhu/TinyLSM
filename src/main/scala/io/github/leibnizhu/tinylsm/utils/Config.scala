package io.github.leibnizhu.tinylsm.utils

import java.io.{File, FileInputStream}
import java.util.Properties

enum Config(private val envName: String, val defaultVal: String) {

  private val sysPropName = Config.toPropertyName(envName)

  case Port extends Config("TINY_LSM_PORT", "9527")
  case Host extends Config("TINY_LSM_LISTEN", "0.0.0.0")
  case BlockSize extends Config("TINY_LSM_BLOCK_SIZE", "4096")
  case TargetSstSize extends Config("TINY_LSM_TARGET_SST_SIZE", (2 << 20).toString)
  case MemTableLimitNum extends Config("TINY_LSM_MEMTABLE_NUM", "50")
  // TODO Compaction配置
  //  compactionOptions: CompactionOptions,
  case EnableWal extends Config("TINY_LSM_ENABLE_WAL", "true")
  case Serializable extends Config("TINY_LSM_SERIALIZABLE", "true")
  case DataDir extends Config("TINY_LSM_DATA_DIR", "/etc/tinylsm/data")


  /**
   * 优先级：
   * SystemProperty > Environment > .env > ConfigFile
   */
  def get(): String = {
    val sysProp = System.getProperty(sysPropName)
    if (sysProp != null && sysProp.nonEmpty) {
      return sysProp
    }
    val envProp = System.getenv(envName)
    if (envProp != null && envProp.nonEmpty) {
      return sysProp
    }
    val envFileProp = Config.envFileProperties.getProperty(envName)
    if (envFileProp != null && envFileProp.nonEmpty) {
      return envFileProp
    }
    val configFileProp = Config.configFileProperties.getProperty(sysPropName)
    if (configFileProp != null && configFileProp.nonEmpty) {
      return configFileProp
    }
    defaultVal
  }

  def getInt: Int = get().toInt

  def getBoolean: Boolean = get().toBoolean
}

object Config {
  private val envFileProperties = loadEnvFile()
  private val configFileProperties = loadConfigFile()

  private def toPropertyName(str: String): String = str.replaceAll("^TINY_LSM_", "").replace("_", ".").toLowerCase

  private def loadEnvFile(): Properties = {
    val prop = Properties()
    val envFileStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(".env")
    if (envFileStream != null) {
      prop.load(envFileStream)
    }
    prop
  }

  private def loadConfigFile(): Properties = {
    val configFileEnvName = "TINY_LSM_CONFIG_FILE"
    val configFileSysPropName = toPropertyName(configFileEnvName)
    val configFile = System.getProperty(configFileSysPropName,
      System.getenv().getOrDefault(configFileEnvName, "/etc/tinylsm/tinylsm.conf"))
    val prop = Properties()
    if (new File(configFile).exists()) {
      prop.load(new FileInputStream(configFile))
    }
    prop
  }

  def print(): Unit = {
    println("TinyLsm configurations:")
    Config.values.foreach(c => println(s"\t${c.sysPropName} => ${c.get()}"))
  }
}