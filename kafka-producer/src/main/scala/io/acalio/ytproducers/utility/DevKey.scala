package io.acalio.ytproducers.utility
import scala.util.Sorting
import java.time.LocalDateTime

class DevKey (
  val key: String
) {
  var isActive: Boolean = true

  var suspendedUntil = None : Option[LocalDateTime]

  /**
    * Suspend the key for 25 hours
    * 
    */
  def suspend() = {
    isActive = false
    //compute the time of wake up - now() + 25 hours 
    val until = LocalDateTime.now().plusDays(1).plusHours(1)
    suspendedUntil = Some(until)
  }


  /**
    * It returns true either if the key is 
    * currently active or the it can be activated as 
    * as its suspesion time is expired
    *
    * @return
    */
  def maybeActivate(): Boolean  = {
    suspendedUntil match {
      case Some(value) => {
        //check if the current time is after the wake up time
        isActive = value.isBefore(LocalDateTime.now())
      }
      case None => {}
    }
    if (isActive)
      suspendedUntil = None
    return isActive
  }

  override def toString(): String = {
    return s"Key: $key, is active: $isActive, until:"// ${suspendedUntil.get}"
  }
}

object DevKeyOrdering extends Ordering[DevKey] {
    /**
    * Elements are sorted according to their 
    * supsendedUntil time and active status.
    * Specifically, three possible scenarios 
    * can be devised: 
    *  1. x,y are both active -> sort by their key string
    *  2. x is active but y is not -> x < y
    *  3. x,y are both suspended -> sort accordingly to their wake up time
    * 
    */
  def compare(x: DevKey, y: DevKey): Int = {
    if(x.isActive && y.isActive) return x.key compare y.key
    if(x.isActive && !y.isActive ) return -1
    if(!x.isActive && y.isActive) return 1
    if(x.suspendedUntil.get.isBefore(y.suspendedUntil.get))
      return -1
    return 1
  }
}
