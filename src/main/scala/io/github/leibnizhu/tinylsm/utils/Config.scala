package io.github.leibnizhu.tinylsm.utils

import java.io.{File, FileInputStream}
import java.util.Properties

enum Config(private val name: String, val defaultVal: String) {
  private val envName = "TINY_LSM_" + name.toUpperCase

  private val sysPropName = Config.toPropertyName(name)

  case HttpPort extends Config("HTTP_PORT", "9527")
  case GrpcPort extends Config("GRPC_PORT", "9526")
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

  // Compressor 配置
  case CompressorType extends Config("COMPRESSOR_TYPE", "zstd")
  case CompressorZstdLevel extends Config("ZSTD_LEVEL", "3")
  case CompressorZstdTrainDict extends Config("ZSTD_TRAIN_DICT", "true")
  case CompressorZstdSampleSize extends Config("ZSTD_SAMPLE_SIZE", (1024 * 1024).toString)
  case CompressorZstdDictSize extends Config("ZSTD_DICT_SIZE", (16 * 1024).toString)
  case CompressorZlibLevel extends Config("ZLIB_LEVEL", "-1")
  case CompressorLz4Level extends Config("LZ4_LEVEL", "-1")
  
  case RaftPersistorType extends Config("RAFT_PERSISTOR", "file")

  /**
   * 优先级：
   * SystemProperty > Environment > .env > ConfigFile
   */
  def get(): String = getOption.getOrElse(defaultVal)

  def getInt: Int = get().toInt

  def getBoolean: Boolean = get().toBoolean

  def isDefined: Boolean = getOption.isDefined

  def unDefined: Boolean = getOption.isEmpty

  private def getOption: Option[String] = {
    val sysProp = System.getProperty(sysPropName)
    if (sysProp != null && sysProp.nonEmpty) {
      return Some(sysProp)
    }
    val envProp = System.getenv(envName)
    if (envProp != null && envProp.nonEmpty) {
      return Some(sysProp)
    }
    val envFileProp = Config.envFileProperties.getProperty(envName)
    if (envFileProp != null && envFileProp.nonEmpty) {
      return Some(envFileProp)
    }
    val configFileProp = Config.configFileProperties.getProperty(sysPropName)
    if (configFileProp != null && configFileProp.nonEmpty) {
      return Some(configFileProp)
    }
    None
  }
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
    val configFile = configFilePath
    val prop = Properties()
    if (new File(configFile).exists()) {
      prop.load(new FileInputStream(configFile))
    }
    prop
  }

  def configFilePath: String = {
    val configFileEnvName = "TINY_LSM_CONFIG_FILE"
    val configFileSysPropName = toPropertyName("CONFIG_FILE")
    val configFile = System.getProperty(configFileSysPropName,
      System.getenv().getOrDefault(configFileEnvName, "/etc/tinylsm/tinylsm.conf"))
    configFile
  }

  def print(): Unit = {
    println("TinyLsm configurations:")
    Config.values.foreach(c => println(s"\t${c.sysPropName} => ${c.get()}"))
  }
}