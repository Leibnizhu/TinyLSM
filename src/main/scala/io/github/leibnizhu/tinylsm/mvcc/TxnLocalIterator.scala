package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.StorageIterator
import io.github.leibnizhu.tinylsm.{MemTableEntry, MemTableValue, RawKey}

import java.util.concurrent.ConcurrentSkipListMap

class TxnLocalIterator(
                        // skipmap 的引用
                        map: ConcurrentSkipListMap[Array[Byte], MemTableValue],
                        /// skipmap 迭代器
                        iter: Iterator[MemTableEntry],
                        /// 存储当前的 key-value 对.
                        var item: (Array[Byte], MemTableValue),
                      ) extends StorageIterator[RawKey] {
  override def key(): RawKey = ???

  override def value(): MemTableValue = ???

  override def isValid: Boolean = ???

  override def next(): Unit = ???
}
