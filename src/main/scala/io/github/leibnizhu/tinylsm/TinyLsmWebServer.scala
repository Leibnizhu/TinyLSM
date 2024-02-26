package io.github.leibnizhu.tinylsm

import cask.model.Response
import org.jline.reader.impl.completer.{AggregateCompleter, ArgumentCompleter, NullCompleter, StringsCompleter}
import org.jline.reader.{Completer, EndOfFileException, LineReaderBuilder, UserInterruptException}
import org.jline.terminal.TerminalBuilder
import requests.{RequestFailedException, get}

import java.io.{File, FileInputStream}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

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
  def deleteByKey(key: String): Unit = {
    storage.delete(key)
  }

  @cask.post("/key/:key")
  def putValue(key: String, value: String): Unit = {
    storage.put(key, value)
  }

  @cask.get("/scan")
  def scan(): Unit = {
    //TODO
  }

  Config.print()
  initialize()
}

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

object TinyLsmCli {

  def main(args: Array[String]): Unit = {
    val argMap = parseArgs(args)
    val cliContext = CliContext(argMap)
    val terminal = TerminalBuilder.builder().name("TinyLsm cli").system(true).build
    val lineReader = LineReaderBuilder.builder().terminal(terminal).appName("TinyLsm cli").completer(getCompleter).build

    // REPL 循环
    while (true) {
      try {
        // 输入命令提示信息, 获取输入的信息
        val line = lineReader.readLine("TinyLsm> ")
        if (null != line && line.trim.nonEmpty) {
          // 解析输入的命令 解析成list
          val words = lineReader.getParsedLine.words();
          executeCommand(words.asScala, cliContext)
        }
      } catch
        case e: UserInterruptException => gracefullyExit()
        case e: EndOfFileException => gracefullyExit()
    }
  }

  private def gracefullyExit(): Unit = {
    println("Please use :quit next time ^_^")
    System.exit(1)
  }

  private def executeCommand(words: mutable.Buffer[String], cliContext: CliContext): Unit = {
    words.head match
      case ":quit" => System.exit(0)
      case ":help" => printHelp()
      case "get" => if (words.length < 2) {
        println("Invalid command, use: get <key>")
      } else {
        cliContext.get(words(1))
      }
      case "delete" => if (words.length < 2) {
        println("Invalid command, use: delete <key>")
      } else {
        cliContext.delete(words(1))
      }
      case "put" => if (words.length < 3) {
        println("Invalid command, use: put <key> <value>")
      } else {
        cliContext.put(words(1), words(2))
      }
      case _ => println("Unsupported command: " + words.head)
  }

  private def printHelp(): Unit = println(
    """
      |Help
      |  get <key> : Get value by key.
      |  delete <key> : Delete a key.
      |  put <key> <value> : Put value by key.
      |  :help : Show this help info.
      |  :quit : Quit TinyLsm cli.""".stripMargin)

  private def getCompleter: Completer =
    new AggregateCompleter(
      new ArgumentCompleter(new StringsCompleter("get"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("put"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("delete"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("scan"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter(":help"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter(":quit"), NullCompleter.INSTANCE),
    )

  private def parseArgs(args: Array[String]): Map[String, Any] = {
    if (args == null || args.isEmpty) {
      Map()
    } else {
      var i = 0
      val result = mutable.HashMap[String, Any]()
      while (i < args.length) {
        val cur = args(i)
        cur match
          case "--playground" =>
            result.put("playground", true)
          case "-h" =>
            result.put("host", args(i + 1))
            i += 1
          case "-p" =>
            result.put("port", args(i + 1).toInt)
            i += 1
          case _ =>
            println("Unsupported argument: " + cur)
        i += 1
      }
      result.toMap
    }
  }
}

class CliContext(playgroundMode: Boolean,
                 playgroundLsm: Option[TinyLsm],
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
}

object CliContext {
  def apply(argMap: Map[String, Any]): CliContext = {
    val playgroundMode = argMap.getOrElse("playground", false).asInstanceOf[Boolean]
    val playgroundLsm = if (playgroundMode) {
      val tempDir = System.getProperty("java.io.tmpdir") + File.separator + "TinyLsmPlayground"
      Some(TinyLsm(new File(tempDir), LsmStorageOptions.defaultOption()))
    } else {
      None
    }
    new CliContext(
      playgroundMode,
      playgroundLsm,
      argMap.getOrElse("host", "localhost").asInstanceOf[String],
      argMap.getOrElse("port", Config.Port.defaultVal.toInt).asInstanceOf[Int]
    )
  }
}