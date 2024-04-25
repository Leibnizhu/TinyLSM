scalaVersion := "3.3.1"
name := "ScalaTinyLsm"
version := "0.4-SNAPSHOT"
Test / parallelExecution := false

lazy val akkaHttpVersion = "10.6.2"
lazy val akkaVersion = "2.9.2"

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

//scalacOptions := Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
  "org.jboss.slf4j" % "slf4j-jboss-logging" % "1.2.1.Final",
  "org.apache.logging.log4j" % "log4j-api" % "2.23.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.23.0",
  "org.jline" % "jline" % "3.25.1",
  "com.github.luben" % "zstd-jni" % "1.5.6-2",
  "org.lz4" % "lz4-java" % "1.8.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3",
  "com.lihaoyi" %% "cask" % "0.9.2",
  "com.lihaoyi" %% "requests" % "0.8.0",
  "com.github.blemale" %% "scaffeine" % "5.2.1",
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.mockito" % "mockito-core" % "4.11.0" % Test
)

lazy val app = (project in file("."))
  .settings(
    assembly / mainClass := Some("io.github.leibnizhu.tinylsm.ap.TinyLsmWebServer"),
    assembly / assemblyJarName := "TinyLsmAssembly.jar",
  )

assembly / assemblyMergeStrategy := {
  case x if x.endsWith("module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

enablePlugins(JmhPlugin)
