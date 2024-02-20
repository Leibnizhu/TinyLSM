package io.github.leibnizhu.tinylsm

class Compact {

}

sealed class CompactionOptions

case class NoCompaction() extends CompactionOptions

case class LeveledCompactionOptions() extends CompactionOptions

case class TieredCompactionOptions() extends CompactionOptions

case class SimpleCompactionOptions() extends CompactionOptions