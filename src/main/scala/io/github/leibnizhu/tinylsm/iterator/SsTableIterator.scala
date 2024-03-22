package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.block.BlockIterator
import io.github.leibnizhu.tinylsm.utils.{Bound, Excluded, Included, Unbounded}

class SsTableIterator(
                       val table: SsTable,
                       var blockItr: BlockIterator,
                       var blockIndex: Int
                     ) extends MemTableStorageIterator {

  override def key(): MemTableKey = blockItr.key()

  override def value(): MemTableValue = blockItr.value()

  override def isValid: Boolean = blockItr.isValid

  override def next(): Unit = {
    blockItr.next()
    if (!blockItr.isValid) {
      // 当前BlockIterator迭代完毕，换下一个
      blockIndex += 1
      if (blockIndex < table.numOfBlocks()) {
        blockItr = BlockIterator.createAndSeekToFirst(table.readBlockCached(blockIndex))
      }
    }
  }

  def seekToFirst(): Unit = {
    blockIndex = 0
    blockItr = BlockIterator.createAndSeekToFirst(table.readBlockCached(0))
  }

  def seekToKey(key: MemTableKey): Unit = {
    val (iter, index) = SsTableIterator.seekToKey(table, key)
    this.blockItr = iter
    this.blockIndex = index
  }
}

object SsTableIterator {
  def createAndSeekToFirst(table: SsTable): SsTableIterator = {
    val iterator = BlockIterator.createAndSeekToFirst(table.readBlockCached(0))
    SsTableIterator(table, iterator, 0)
  }

  def createAndSeekToKey(table: SsTable, key: MemTableKey): SsTableIterator = {
    val (iterator, index) = seekToKey(table, key)
    SsTableIterator(table, iterator, index)
  }

  def seekToKey(table: SsTable, key: MemTableKey): (BlockIterator, Int) = {
    var blockIndex = table.findBlockIndex(key)
    var block = table.readBlockCached(blockIndex)
    var blockIter = BlockIterator.createAndSeekToKey(block, key)
    if (!blockIter.isValid) {
      blockIndex += 1
      if (blockIndex < table.numOfBlocks()) {
        block = table.readBlockCached(blockIndex)
        blockIter = BlockIterator.createAndSeekToFirst(block)
      }
    }
    (blockIter, blockIndex)
  }

  def createByLowerBound(sst: SsTable, lower: Bound): SsTableIterator = lower match {
    // 没有左边界，则直接到最开始遍历
    case Unbounded() => SsTableIterator.createAndSeekToFirst(sst)
    // 包含左边界，则可以跳到左边界的key开始遍历
    case Included(l: MemTableKey) => SsTableIterator.createAndSeekToKey(sst, MemTableKey.withBeginTs(l))
    // 不包含左边界，则先跳到左边界的key，如果跳完之后实际的key等于左边界，由于不包含边界所以跳到下个值
    case Excluded(l: MemTableKey) =>
      val iter = SsTableIterator.createAndSeekToKey(sst, MemTableKey.withBeginTs(l))
      if (iter.isValid && iter.key().equalsOnlyKey(l)) {
        iter.next()
      }
      iter
    case _ => throw new IllegalArgumentException("Unsupported Bound type: " + lower.getClass)
  }
}