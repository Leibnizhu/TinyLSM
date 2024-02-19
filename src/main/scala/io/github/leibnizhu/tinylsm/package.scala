package io.github.leibnizhu

import io.github.leibnizhu.tinylsm.StorageIterator

package object tinylsm {
  type MemTableValue = Array[Byte]
  type MemTableEntry = java.util.Map.Entry[ByteArrayKey, Array[Byte]]
  type MemTableStorageIterator = StorageIterator[Array[Byte], MemTableValue]
}
