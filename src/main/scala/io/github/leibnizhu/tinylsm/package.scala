package io.github.leibnizhu

import io.github.leibnizhu.tinylsm.StorageIterator

import scala.util.hashing.MurmurHash3

package object tinylsm {
  type MemTableKey = Array[Byte]
  type MemTableValue = Array[Byte]
  type MemTableEntry = java.util.Map.Entry[ByteArrayKey, MemTableValue]
  type MemTableStorageIterator = StorageIterator[MemTableKey, MemTableValue]

  def byteArrayCompare(a: Array[Byte], b: Array[Byte]): Int = {
    if (a eq b) {
      return 0
    }
    if (a == null || b == null) {
      return if (a == null) -1 else 1
    }

    val i = byteArrayMismatch(a, b, Math.min(a.length, b.length))
    if (i >= 0) {
      return java.lang.Byte.compare(a(i), b(i))
    }
    a.length - b.length
  }

  def byteArrayMismatch(a: Array[Byte], b: Array[Byte], length: Int): Int = {
    var i: Int = 0
    while (i < length) {
      if (a(i) != b(i)) {
        return i
      }
      i += 1
    }
    -1
  }

  def byteArrayHash(bytes: Array[Byte]): Int = {
    MurmurHash3.seqHash(bytes)
  }
}
