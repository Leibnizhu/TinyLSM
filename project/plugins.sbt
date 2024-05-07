resolvers += "Akka library repository".at("https://repo.akka.io/maven")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.7")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.4.1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.1.1")