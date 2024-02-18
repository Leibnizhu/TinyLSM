package io.github.leibnizhu.tinylsm

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * MemTable。
 * 其大小增长到 LsmStorageOptions.targetSstSize 之后，需要冻结，并flush到磁盘
 *
 * @param id              MemTable唯一标识，应该是自增的，或至少是单调增的
 * @param map             内部存储的Map，Key用了List[Byte]，是为了利用List有效的equals方法；否则如果用Array[Byte]，equals和hashCode使用的是对象ID
 * @param wal             可选的WriteAheadLog
 * @param approximateSize 记录当前的预估大小
 */
case class MemTable(
                     id: Int,
                     map: ConcurrentHashMap[List[Byte], Array[Byte]],
                     wal: Option[WriteAheadLog],
                     val approximateSize: AtomicInteger) {

  /**
   * 按key获取
   *
   * @param key key
   * @return 可能为None
   */
  def get(key: Array[Byte]): Option[Array[Byte]] = {
    Option(map.get(key.toList))
  }

  /**
   * 插入或更新
   *
   * @param key   key
   * @param value 如果要执行delete操作，可以传入空Array
   */
  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    val estimateSize = key.length + (if (value == null) 0 else value.length)
    map.put(key.toList, value)
    approximateSize.addAndGet(estimateSize)
    wal.foreach(_wal => _wal.put(key, value))
  }
}

object MemTable {
  /**
   *
   * @param id      MemTable唯一标识，应该是自增的，或至少是单调增的
   * @param walPath WriteAheadLog的存储路径；默认不做WAL
   * @return MemTable实例
   */
  def apply(id: Int, walPath: Option[File] = None): MemTable = new MemTable(id,
    new ConcurrentHashMap[List[Byte], Array[Byte]](),
    walPath.map(p => WriteAheadLog(p)),
    AtomicInteger(0))
}