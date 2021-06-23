ThisBuild / scalaVersion := "2.12.12"
lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"


lazy val kafka = "org.apache.kafka" % "kafka-clients" % "2.6.0"
lazy val youtube = "com.google.apis" % "google-api-services-youtube" % "v3-rev222-1.25.0"
lazy val log = "ch.qos.logback" % "logback-classic" % "0.9.28"
lazy val dataBind = "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.2" 
lazy val avro =  "org.apache.avro" % "avro" % "1.10.0"
lazy val confluent = "io.confluent" % "kafka-avro-serializer" % "5.5.1"
lazy val scallop = "org.rogach" %% "scallop" % "3.5.1"
lazy val redis = "redis.clients" % "jedis" % "3.5.1"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

lazy val app = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "Kafka-YT-Producer",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      log,
      kafka,
      dataBind,
      youtube,
      avro,
      confluent,
      scallop,
      redis
    )
  )



