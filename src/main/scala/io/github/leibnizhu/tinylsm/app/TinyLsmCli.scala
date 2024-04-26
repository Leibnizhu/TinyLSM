package io.github.leibnizhu.tinylsm.app

import org.jline.reader.impl.completer.{AggregateCompleter, ArgumentCompleter, NullCompleter, StringsCompleter}
import org.jline.reader.{Completer, EndOfFileException, LineReaderBuilder, UserInterruptException}
import org.jline.terminal.TerminalBuilder

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

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
        val line = lineReader.readLine(cliContext.cliPrompt())
        if (null != line && line.trim.nonEmpty) {
          // 解析输入的命令 解析成list
          val words = lineReader.getParsedLine.words();
          executeCommand(words.asScala, cliContext)
        }
      } catch
        case _: UserInterruptException | _: EndOfFileException => gracefullyExit()
        case e: Exception => e.printStackTrace()
    }
  }

  private def gracefullyExit(): Unit = {
    println("Please use :quit next time ^_^")
    System.exit(1)
  }

  private def executeCommand(words: mutable.Buffer[String], cliContext: CliContext): Unit = {
    words.head match
      case ":quit" | ":exit" => System.exit(0)
      case ":help" => printHelp()
      case "get" => if (words.length < 2) {
        println("Invalid command, use: get <key>")
      } else {
        cliContext.get(words(1))
      }
      case "delete" | "del" => if (words.length < 2) {
        println("Invalid command, use: delete <key>")
      } else {
        cliContext.delete(words(1))
      }
      case "put" => if (words.length < 3) {
        println("Invalid command, use: put <key> <value>")
      } else {
        cliContext.put(words(1), words(2))
      }
      case "scan" => if (words.length < 5) {
        println("Invalid command, use: scan <Unbound|Excluded|Included> <fromKey> <Unbound|Excluded|Included> <toKey>")
      } else {
        cliContext.scan(words(1), words(2), words(3), words(4))
      }
      case "txn" => cliContext.newTxn()
      case "commit" => cliContext.commitCurrentTxn()
      case "rollback" => cliContext.rollbackCurrentTxn()
      case "flush" => cliContext.flush()
      case "status" => cliContext.status()
      case _ => println(s"Unsupported command: '${words.head}', you can type :help for more information or <TAB> for auto complete")
  }

  private def printHelp(): Unit = println(
    """
      |TinyLSM Help
      |Cli args:
      |  --playground : playground mode. start a temp internal TinyLSM server and connect to it.
      |  --debug : enable debug mode. flush etc. command can be use.
      |  --help: show help info.
      |  -h: TinyLSM host, default value is localhost.
      |  -p: TinyLSM port, default value is 9527.
      |Commands:
      |  get <key> : Get value by key.
      |  delete <key> : Delete a key.
      |  put <key> <value> : Put value by key.
      |  scan <Unbound|Excluded|Included> <fromKey> <Unbound|Excluded|Included> <toKey> : Scan by key range.
      |  flush : Force flush MemTable to SST.
      |  status : Show TinyLSM status.
      |  txn: Start a new transaction
      |  commit: Commit current transaction
      |  rollback: rollback current transaction
      |  :help : Show this help info.
      |  :quit or :exit : Quit TinyLsm cli.""".stripMargin)

  private def getCompleter: Completer =
    val boundCompleter = new StringsCompleter("Unbounded", "Excluded", "Included")
    new AggregateCompleter(
      new ArgumentCompleter(new StringsCompleter("get"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("put"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("delete"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("scan"), boundCompleter, NullCompleter.INSTANCE, boundCompleter, NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("flush"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("status"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("txn"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("commit"), NullCompleter.INSTANCE),
      new ArgumentCompleter(new StringsCompleter("rollback"), NullCompleter.INSTANCE),
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
          case "--debug" =>
            result.put("debug", true)
          case "-h" =>
            result.put("host", args(i + 1))
            i += 1
          case "-p" =>
            result.put("port", args(i + 1).toInt)
            i += 1
          case "--help" =>
            printHelp()
            System.exit(0)
          case _ =>
            println("Unsupported argument: " + cur)
        i += 1
      }
      result.toMap
    }
  }
}
