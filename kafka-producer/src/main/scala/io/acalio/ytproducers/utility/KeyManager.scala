package io.acalio.ytproducers.utility
import collection.mutable.PriorityQueue
/**
  * This class stores all the developer key to be used 
  * when using the YoutubeDataManager. 
  * A key is used until it expires - it reaches the quota limit. 
  * 
  */
class KeyManager(
  val keys: Seq[DevKey]
) {

  //priorityQueue of available keys
  var keyQueue: PriorityQueue[DevKey] = new PriorityQueue[DevKey]()(DevKeyOrdering.reverse)
  keyQueue.enqueue(keys:_*)

  var currentKey = None : Option[DevKey]

  def getKey(): String = {
    var popNewKey = false
    currentKey match {
      case None => {
        popNewKey = true
      }
      case Some(value) => {
        if(!value.isActive)
          popNewKey = true
      }
    }

    if(popNewKey) {
      //pop the first key ini the queue
      currentKey = Some(keyQueue.dequeue())
      //try to activate the key
      val activated: Boolean = currentKey.get.maybeActivate()
      //if it is still inactive there is not any actve key
      if(!activated) {
        //re insert the current Key
        keyQueue.enqueue(currentKey.get)
        //set the current Key to None
        val until = currentKey.get.suspendedUntil.get
        currentKey = None
        throw  KeyException(until)
      }
    }

    return currentKey.get.key
  }

  /**
    * This method blocks the current key and it increases
    * the curreninidex
    * 
    **/
  def blockKey() {
    if(currentKey.isDefined) {
    //suspend the current key
      currentKey.get.suspend()
      //insert the key into the priority queue again
      keyQueue.enqueue(currentKey.get)
    }
  }
}

