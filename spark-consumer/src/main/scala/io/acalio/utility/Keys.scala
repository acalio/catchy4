package io.acalio.utility

object Keys {
  //s3
  val S3_ACCESS_KEY: String = "fs.s3a.access.key"
  val S3_SECRET_KEY: String = "fs.s3a.secret.key"
  val S3_ENDPOINT: String = "fs.s3a.endpoint"
  val S3_SOURCE_BASE: String = "s3.source.basepath"

  val SOURCE = "fs.source"
  val SINK = "fs.sink"
  // val S3_SINK_BASE: String = "s3.sink.basepath"
  // val S3_SOURCE: String = "s3.source"
  // val S3_SINK: String = "s3.sink"

  //kafka configuration
  val BROKER_URL: String = "kafka.bootstrap.servers"
  val TOPIC: String = "kafka.subscribe"
  val OFFSET: String= "kafka.startingOffsets"

  //schema registriy configuration
  val SCHEMA_REGISTRY_URL = "schemaRegistry.url"

  //application properties
  val ENTITY = "entity"

}
