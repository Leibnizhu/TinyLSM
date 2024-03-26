package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.{FusedIterator, TwoMergeIterator}
import io.github.leibnizhu.tinylsm.{MemTableKey, MemTableStorageIterator, MemTableValue}

class TxnIterator(
                   _txn: Transaction,
                   iter: TwoMergeIterator[TxnLocalIterator, FusedIterator[MemTableKey, MemTableValue]]
                 ) extends MemTableStorageIterator {
  override def key(): MemTableKey = iter.key()

  override def value(): MemTableValue = iter.value()

  override def isValid: Boolean = iter.isValid

  override def next(): Unit = ???

  override def numActiveIterators(): Int = iter.numActiveIterators()
}
