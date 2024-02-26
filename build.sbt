scalaVersion := "3.3.1"
name := "ScalaTinyLsm"
version := "0.2-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "cask" % "0.9.2" % "compile",
  "com.lihaoyi" %% "requests" % "0.8.0"% "compile",
  "com.github.blemale" %% "scaffeine" % "5.2.1" % "compile",
  "org.jboss.slf4j" % "slf4j-jboss-logging" % "1.2.1.Final" % "compile",
  "org.jline" % "jline" % "3.25.1" % "compile",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.mockito" % "mockito-core" % "4.11.0" % Test
)

lazy val app = (project in file("."))
  .settings(
    assembly / mainClass := Some("io.github.leibnizhu.tinylsm.TinyLsmWebServer"),
    assembly / assemblyJarName := "TinyLsmAssembly.jar",
  )
