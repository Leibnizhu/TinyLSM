package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.{MemTableKey, MemTableStorageIterator, MemTableValue}

class TxnLocalIterator(

                      ) extends MemTableStorageIterator {
  override def key(): MemTableKey = ???

  override def value(): MemTableValue = ???

  override def isValid: Boolean = ???

  override def next(): Unit = ???
}
