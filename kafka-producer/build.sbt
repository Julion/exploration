
name := "kafka-producer"

version := "1.0"

scalaVersion := "2.11.8"


assemblyOption in assembly ~= { _.copy(includeScala = false) }

libraryDependencies ++= Seq(
  "net.cakesolutions" %% "scala-kafka-client" % "0.10.0.0"
)
