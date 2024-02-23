scalaVersion := "3.3.1"
name := "ScalaTinyLsm"
version := "0.1"

libraryDependencies ++= Seq(
//  "com.github.ben-manes.caffeine" %% "caffeine" % "3.1.8",
  "com.github.blemale" %% "scaffeine" % "5.2.1" % "compile",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.mockito" % "mockito-core" % "4.11.0" % Test
)
