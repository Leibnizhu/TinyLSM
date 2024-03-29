package io.github.leibnizhu.tinylsm.utils

import java.io.{File, FileInputStream}
import java.util.Properties

enum Config(private val name: String, val defaultVal: String) {
  private val envName = "TINY_LSM_" + name.toUpperCase

  private val sysPropName = Config.toPropertyName(name)

  case Port extends Config("PORT", "9527")
  case Host extends Config("LISTEN", "0.0.0.0")
  case BlockSize extends Config("BLOCK_SIZE", "4096")
  case TargetSstSize extends Config("TARGET_SST_SIZE", (2 << 20).toString)
  case TargetManifestSize extends Config("TARGET_MANIFEST_SIZE", (1 << 20).toString)
  case MemTableLimitNum extends Config("MEMTABLE_NUM", "50")
  case EnableWal extends Config("ENABLE_WAL", "true")
  case Serializable extends Config("SERIALIZABLE", "true")
  case DataDir extends Config("DATA_DIR", "/etc/tinylsm/data")

  // Compaction配置
  case CompactionStrategy extends Config("COMPACTION_STRATEGY", "leveled")
  case CompactionMaxLevels extends Config("COMPACTION_MAX_LEVELS", "5")
  case CompactionLevel0FileNumTrigger extends Config("COMPACTION_LEVEL0_FILE_NUM_TRIGGER", "5")
  case CompactionSizeRatioPercent extends Config("COMPACTION_SIZE_RATIO_PERCENT", "200")
  case CompactionLevelSizeMultiplier extends Config("COMPACTION_LEVEL_SIZE_MULTIPLIER", "4")
  case CompactionBaseLevelSizeMb extends Config("COMPACTION_BASE_LEVEL_SIZE_MB", "100")
  case CompactionMaxSizeAmpPercent extends Config("COMPACTION_MAX_SIZE_AMP_PERCENT", "200")
  case CompactionMinMergeWidth extends Config("COMPACTION_MIN_MERGE_WIDTH", "2")


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

  private def toPropertyName(str: String): String = str.replace("_", ".").toLowerCase

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
    val configFileSysPropName = toPropertyName("CONFIG_FILE")
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