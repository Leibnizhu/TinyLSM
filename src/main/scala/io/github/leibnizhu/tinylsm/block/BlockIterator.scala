package io.github.leibnizhu.tinylsm.block

import io.github.leibnizhu.tinylsm.*

class BlockIterator(block: Block) extends MemTableStorageIterator {
  private var index: Int = 0;
  /**
   * 当前迭代到的key，初始化和迭代完之后都是 None
   */
  private var curKey: Option[MemTableKey] = None
  /**
   * 当前value在Block中data的下标
   */
  private var curValuePos: (Int, Int) = (0, 0)
  private val firstKey = block.getFirstKey()

  def seekToFirst(): Unit = {
    seekToIndex(0)
  }

  /**
   * 跳到 >=指定key的位置
   *
   * @param key 指定定位的key，
   */
  def seekToKey(key: MemTableKey): Unit = {
    // key是有序存储的，可以用二分法
    var low = 0
    var high = block.offsets.length
    while (low < high) {
      val mid = low + (high - low) / 2
      seekToIndex(mid)
      assert(isValid)
      val compare = byteArrayCompare(curKey.get, key)
      if (compare < 0) {
        low = mid + 1
      } else if (compare > 0) {
        high = mid
      } else {
        return
      }
    }
    seekToIndex(low)
  }

  override def key(): MemTableKey = {
    assert(isValid, "BlockIterator is invalid")
    curKey.orNull
  }

  override def value(): MemTableValue = {
    assert(isValid, "BlockIterator is invalid")
    block.data.slice(curValuePos._1, curValuePos._2)
  }

  override def isValid: Boolean = {
    curKey.isDefined
  }

  override def next(): Unit = {
    index += 1
    seekToIndex(index)
  }

  /**
   * 跳到上一个
   */
  def prev(): Unit = {
    // index == 0 时不能再往前跳，这算异常吗？还是直接跳过忽略？
    if (index > 0) {
      index -= 1
      seekToIndex(index)
    }
  }

  private def seekToIndex(index: Int): Unit = {
    if (index < 0) {
      throw new IllegalArgumentException("Index must be positive!")
    }
    if (index >= block.offsets.length) {
      // 越界，则不可用
      curKey = None
      curValuePos = (0, 0)
      return
    }

    // 根据 offset 段获取entry位置
    val entryOffset = block.offsets(index)
    // 先后读取overlap长度、剩余key长度、剩余key、value长度
    val overlapLength = low2BytesToInt(block.data(entryOffset), block.data(entryOffset + 1))
    val restKeyLength = low2BytesToInt(block.data(entryOffset + 2), block.data(entryOffset + 3))
    curKey = Some(firstKey.slice(0, overlapLength) ++
      block.data.slice(entryOffset + 4, entryOffset + 4 + restKeyLength))
    val valueOffset = entryOffset + 4 + restKeyLength
    val valueLength = low2BytesToInt(block.data(valueOffset), block.data(valueOffset + 1))
    curValuePos = (valueOffset + 2, valueOffset + 2 + valueLength)
    this.index = index
  }
}

object BlockIterator {

  def createAndSeekToFirst(block: Block): BlockIterator = {
    val itr = new BlockIterator(block)
    itr.seekToFirst()
    itr
  }

  def createAndSeekToKey(block: Block, key: MemTableKey): BlockIterator = {
    val itr = new BlockIterator(block)
    itr.seekToKey(key)
    itr
  }

}