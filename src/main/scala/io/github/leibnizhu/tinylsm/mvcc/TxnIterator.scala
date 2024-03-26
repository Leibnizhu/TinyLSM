package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.{FusedIterator, StorageIterator, TwoMergeIterator}
import io.github.leibnizhu.tinylsm.{MemTableValue, RawKey}

class TxnIterator(
                   _txn: Transaction,
                   iter: TwoMergeIterator[RawKey, TxnLocalIterator, FusedIterator[RawKey]]
                 ) extends StorageIterator[RawKey] {
  override def key(): RawKey = iter.key()

  override def value(): MemTableValue = iter.value()

  override def isValid: Boolean = iter.isValid

  override def next(): Unit = ???

  override def numActiveIterators(): Int = iter.numActiveIterators()
}
