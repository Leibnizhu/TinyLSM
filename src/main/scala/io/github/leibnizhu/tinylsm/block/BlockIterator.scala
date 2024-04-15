package io.github.leibnizhu.tinylsm.block

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.iterator.StorageIterator
import io.github.leibnizhu.tinylsm.utils.ByteArrayReader

/**
 * Block数据的迭代器
 * TODO 实现 prev 方法前向迭代，从而可以反向scan
 *
 * @param block block对象
 */
class BlockIterator(block: Block) extends StorageIterator[MemTableKey] {
  private var index: Int = 0
  /**
   * 当前迭代到的key，初始化和迭代完之后都是 None
   */
  private var curKey: Option[MemTableKey] = None
  /**
   * 当前value在Block中data的下标
   */
  private var curValue: Option[MemTableValue] = None
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
      val compare = curKey.get.compareTo(key)
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
    curValue.orNull
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
      curValue = None
      return
    }

    // 根据 offset 段获取entry位置
    val entryOffset = block.offsets(index)
    // 先后读取overlap长度、剩余key长度、剩余key、value长度
    val blockData = new ByteArrayReader(block.data).seekTo(entryOffset)
    val overlapLength = blockData.readUint16()
    val restKeyLength = blockData.readUint16()
    val keyBytes = firstKey.bytes.slice(0, overlapLength) ++ blockData.readBytes(restKeyLength)
    val ts = blockData.readUint64()
    curKey = Some(MemTableKey(keyBytes, ts))
    val valueLength = blockData.readUint16()
    val rawValueLength = blockData.readUint16()
    // entry开头+overlap 2B+剩余key 2B+剩余key内容+时间戳 8B+value长度2B+value原始长度2B
    val valueOffset = entryOffset + SIZE_OF_U16 * 2 + restKeyLength + SIZE_OF_LONG + SIZE_OF_U16 * 2
    val rawValue = block.data.slice(valueOffset, valueOffset + valueLength)
    curValue = Some(block.compressor.decompress(rawValue, rawValueLength))
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