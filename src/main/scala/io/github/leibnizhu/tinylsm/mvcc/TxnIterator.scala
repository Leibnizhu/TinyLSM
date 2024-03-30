package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.{FusedIterator, StorageIterator, TwoMergeIterator}
import io.github.leibnizhu.tinylsm.{Key, MemTableValue, RawKey}

type TxnInnerIterator = TwoMergeIterator[RawKey, TxnLocalIterator, FusedIterator[RawKey]]
//type TxnInnerIterator = FusedIterator[RawKey]

class TxnIterator(
                   _txn: Transaction,
                   innerIter: TxnInnerIterator
                 ) extends StorageIterator[RawKey] {
  skipDeletes()
  if (innerIter.isValid) {
    addToReadSet(innerIter.key())
  }

  override def key(): RawKey = innerIter.key()

  override def value(): MemTableValue = innerIter.value()

  override def isValid: Boolean = innerIter.isValid

  override def next(): Unit = {
    innerIter.next()
    skipDeletes()
    if (innerIter.isValid) {
      addToReadSet(innerIter.key())
    }
  }

  override def numActiveIterators(): Int = innerIter.numActiveIterators()

  private def skipDeletes(): Unit = {
    while (innerIter.isValid && innerIter.deletedValue()) {
      innerIter.next()
    }
  }

  private def addToReadSet(key: Key): Unit = if (_txn.keyHashes.isDefined) {
    _txn.keyHashes.get.execute((_, readHashes) => readHashes.add(key.keyHash()))
  }
}
