name := "FIleWorker"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.7",
  "com.typesafe.slick" %% "slick" % "3.2.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.2.1",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.postgresql" % "postgresql" % "42.1.4"
)
