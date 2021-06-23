package io.acalio.ytproducers.utility

object Keys {
  //kafka parameter
  val BROKER_URL: String = "bootstrap.servers"
  val OFFSET: String= "startingOffsets"
  val KEY_SERIALIZER: String = "key.serializer"
  val VALUE_SERIALIZER: String = "value.serializer"

  //schema registriy configuration
  val SCHEMA_REGISTRY_URL: String = "schema.registry.url"

  //application properties
  val TOPIC_NAME: String = "topic"
  val LIMIT_VIDEO: String = "video.limit"
  val LIMIT_COMMENT: String = "comment.limit"
  val LIMIT_SUBSCRIPTION: String = "subscription.limit"
  val LIMIT_LIKE: String = "like.limit"
  val QUERY: String = "query"
  val PREFIX_VIDEO: String = "video-"
  val PREFIX_COMMENT: String = "comment-"
  val PREFIX_SUBSCRIPTION: String = "subscription-"
  val PREFIX_LIKE: String = "like-"
  val PREFIX_CHANNEL: String = "channel-"
  val CHANNEL_ID: String = "channel"
  val VIDEO_ID: String = "video"


}
