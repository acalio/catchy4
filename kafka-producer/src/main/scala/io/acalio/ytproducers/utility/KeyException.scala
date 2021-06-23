package io.acalio.ytproducers.utility
import java.time.LocalDateTime


final case class DevKeyException(
  private val message: String = "No Developer Key are available at the moment",
  private val cause: Throwable = None.orNull,
  val firstCheckPoint: LocalDateTime
) extends Exception(message, cause)
