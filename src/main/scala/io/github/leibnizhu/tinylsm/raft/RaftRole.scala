package io.github.leibnizhu.tinylsm.raft

sealed trait RaftRole {
  def shortName: String
}

case object Follower extends RaftRole:
  override def shortName = "F"

case object Candidate extends RaftRole:
  override def shortName = "C"

case object Leader extends RaftRole:
  override def shortName = "L"