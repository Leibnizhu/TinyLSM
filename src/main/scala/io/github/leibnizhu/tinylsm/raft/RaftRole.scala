package io.github.leibnizhu.tinylsm.raft

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

enum RaftRole(val shortName: String):
  case Follower extends RaftRole("F")
  case Candidate extends RaftRole("C")
  case Leader extends RaftRole("L")