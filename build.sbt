name := "AkkaStreamKafka"
version := "0.0.1"

scalaVersion := "2.12.6"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-Xcheck-null"
)

lazy val akkaVersion = "2.5.12"
lazy val kafkaVersion = "1.1.0"
lazy val logbackVersion = "1.2.3"
lazy val scalatestVersion = "3.0.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
)

unmanagedSources / excludeFilter := HiddenFileFilter || ".#*" || "*~"

fork := true

test / parallelExecution := false
