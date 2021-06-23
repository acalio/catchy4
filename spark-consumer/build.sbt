ThisBuild / scalaVersion := "2.12.12"


lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"
lazy val log = "ch.qos.logback" % "logback-classic" % "0.9.28"
lazy val avro =  "org.apache.avro" % "avro" % "1.10.0"
lazy val confluent =  "io.confluent" % "kafka-avro-serializer" % "5.5.1"
lazy val sparkSQL =  "org.apache.spark" %% "spark-sql" % "3.0.0" 
lazy val sparkKafkasQL = "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0" 
lazy val sparkAvro = "org.apache.spark" %% "spark-avro" % "3.0.0"
lazy val abris =  "za.co.absa" % "abris_2.12" % "4.0.0"
lazy val nio = "nio" % "nio" % "1.0.4"
lazy val javax = "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1"
lazy val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % "2.4.0"
lazy val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "3.3.0"
lazy val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % "3.3.0"
lazy val scallop = "org.rogach" %% "scallop" % "3.5.1"
lazy val coreNlp = "edu.stanford.nlp" % "stanford-corenlp" % "4.0.0"
lazy val tint = "eu.fbk.dh" % "tint-runner" % "0.2" // for italian sentiment analysis
lazy val lingua = "com.github.pemistahl" % "lingua" % "1.0.3"
lazy val sentimentAnalyzer = "io.acalio.nlp" % "sentiment-analyzer" % "1.0"

resolvers ++= Seq(
  "confluent" at "https://packages.confluent.io/maven/",
  Resolver.mavenLocal
)

lazy val app = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "YouTube Spark",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      sparkSQL,
      sparkKafkasQL,
      sparkAvro,
      log,
      abris,
      confluent,
      hadoopAws,
      hadoopClient,
      hadoopCommon,
      scallop,
      coreNlp,
      tint,
      lingua,
      sentimentAnalyzer
    )
  )
