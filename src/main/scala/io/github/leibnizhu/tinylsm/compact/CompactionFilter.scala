package io.github.leibnizhu.tinylsm.compact

enum CompactionFilter {
  case Prefix(bytes: Array[Byte]) extends CompactionFilter
}