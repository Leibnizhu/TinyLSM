package io.github.leibnizhu.tinylsm

package object iterator {

  /**
   * LsmIterator内部包装的迭代器类型
   * 使用TwoMergeIterator ，优先迭代内存的MemTableIterator，再迭代SST的SsTableIterator
   */
  type LsmIteratorInner = TwoMergeIterator[
    TwoMergeIterator[MergeIterator[MemTableIterator], MergeIterator[SsTableIterator]],
    MergeIterator[SstConcatIterator]
  ]

}
