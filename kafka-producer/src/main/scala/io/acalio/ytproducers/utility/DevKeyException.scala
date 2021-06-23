package io.acalio.ytproducers.utility
import java.time.LocalDateTime


final case class KeyException(
  val firstCheckPoint: LocalDateTime
) extends Exception("No Developer Key are available at the moment")
