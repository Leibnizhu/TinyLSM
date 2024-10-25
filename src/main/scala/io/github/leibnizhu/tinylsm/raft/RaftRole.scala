package io.github.leibnizhu.tinylsm.raft

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME, // 使用类型名称
  include = JsonTypeInfo.As.PROPERTY, // 在属性中包含类型
  property = "type" // 类型信息的字段名
)
@JsonSubTypes(value = Array(
  new JsonSubTypes.Type(value = classOf[Follower$], name = "Follower"),
  new JsonSubTypes.Type(value = classOf[Candidate$], name = "Candidate"),
  new JsonSubTypes.Type(value = classOf[Leader$], name = "Leader")
))
sealed trait RaftRole {
  def shortName: String
}

case object Follower extends RaftRole:
  override def shortName = "F"

case object Candidate extends RaftRole:
  override def shortName = "C"

case object Leader extends RaftRole:
  override def shortName = "L"