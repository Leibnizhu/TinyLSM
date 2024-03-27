package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.{FusedIterator, StorageIterator, TwoMergeIterator}
import io.github.leibnizhu.tinylsm.{MemTableValue, RawKey}

//type TxnInnerIterator = TwoMergeIterator[RawKey, TxnLocalIterator, FusedIterator[RawKey]]
type TxnInnerIterator = FusedIterator[RawKey]

class TxnIterator(
                   _txn: Transaction,
                   innerIter: TxnInnerIterator
                 ) extends StorageIterator[RawKey] {
  override def key(): RawKey = innerIter.key()

  override def value(): MemTableValue = innerIter.value()

  override def isValid: Boolean = innerIter.isValid

  override def next(): Unit = {
    innerIter.next()
  }

  override def numActiveIterators(): Int = innerIter.numActiveIterators()
}
