import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"
  lazy val kafka = "org.apache.kafka" % "kafka-clients" % "2.6.0"
  lazy val youtube = "com.google.apis" % "google-api-services-youtube" % "v3-rev222-1.25.0"
  lazy val log = "ch.qos.logback" % "logback-classic" % "0.9.28"
  lazy val dataBind = "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.2"
  lazy val avro =  "org.apache.avro" % "avro" % "1.10.0"
  lazy val confluent = "io.confluent" % "kafka-avro-serializer" % "5.5.1"
  lazy val sparkKafkasQL = "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0" % "provided"
  lazy val sparkAvro = "org.apache.spark" %% "spark-avro" % "3.0.0"
}
