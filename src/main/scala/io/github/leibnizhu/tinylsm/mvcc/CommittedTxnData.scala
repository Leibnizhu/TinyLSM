package io.github.leibnizhu.tinylsm.mvcc

import java.util

case class CommittedTxnData(
                             keyHashes: util.HashSet[Int],
                             readTs: Long,
                             commitTs: Long,
                           ) {

}
