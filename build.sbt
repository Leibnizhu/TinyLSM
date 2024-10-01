
scalaVersion := "3.3.1"
name := "ScalaTinyLsm"
version := "0.4-SNAPSHOT"
Test / parallelExecution := false

val pekkoVersion = "1.1.0-M1"
val log4j2Version = "2.24.1"

resolvers += Resolver.url("typesafe", url("https://repo.typesafe.com/typesafe/ivy-releases/"))(Resolver.ivyStylePatterns)
unmanagedResourceDirectories in Compile += baseDirectory.value / "src" / "main" / "resources"

enablePlugins(PekkoGrpcPlugin)

libraryDependencies ++= Seq(
  // 日志相关
//  "org.jboss.slf4j" % "slf4j-jboss-logging" % "1.2.1.Final",
  "org.apache.logging.log4j" % "log4j-api" % log4j2Version,
  "org.apache.logging.log4j" % "log4j-core" % log4j2Version,
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % log4j2Version,
  "org.slf4j" % "slf4j-api" % "2.0.13",
// cli解析命令
  "org.jline" % "jline" % "3.26.2",
  // 压缩相关
  "com.github.luben" % "zstd-jni" % "1.5.6-4",
  "org.lz4" % "lz4-java" % "1.8.0",
  // jackson
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.0",
  // caffeine 缓存
  "com.github.blemale" %% "scaffeine" % "5.3.0",
  // pekko 相关
  "org.apache.pekko" %% "pekko-http" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-grpc-runtime" % pekkoVersion,
  "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-persistence-typed" % pekkoVersion,

  // pekko 测试相关
  "org.apache.pekko" %% "pekko-http-testkit" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,

  // 其他测试相关
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.mockito" % "mockito-core" % "5.11.0" % Test
)

lazy val app = (project in file("."))
  .settings(
    assembly / mainClass := Some("io.github.leibnizhu.tinylsm.ap.TinyLsmServer"),
    assembly / assemblyJarName := "TinyLsmAssembly.jar",
  )

assembly / assemblyMergeStrategy := {
  case x if x.endsWith("module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

enablePlugins(JmhPlugin)
