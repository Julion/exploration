name := "kafka-producer"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.10.0.0"