package io.github.leibnizhu.tinylsm.mvcc

import java.util

case class CommittedTxnData(
                             // Transaction ID
                             tid: Int,
                             // Transaction 中写入的key的hash值集合
                             keyHashes: util.HashSet[Int],
                             // Transaction 的 readTs 版本
                             readTs: Long,
                             // Transaction 的提交 版本
                             commitTs: Long,
                           ) {

}
