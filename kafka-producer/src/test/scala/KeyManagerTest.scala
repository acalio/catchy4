package io.acalio.ytproducers
import collection.mutable.PriorityQueue
import org.scalatest._
import io.acalio.ytproducers.utility.DevKey
import io.acalio.ytproducers.utility.DevKeyOrdering
import io.acalio.ytproducers.utility.KeyManager
import io.acalio.ytproducers.utility.KeyException

class KeyManagerTest extends UnitSpec {

  "A stack" should "pop values" in {
    val q: PriorityQueue[DevKey] = new PriorityQueue[DevKey]()(DevKeyOrdering.reverse)
    var a = new DevKey("a")
    var b = new DevKey("b")
    var c = new DevKey("c")

    q.enqueue(c,a,b)

    var  p = q.dequeue()
    p.suspend() //suspen the key
    assert(p.key=="a")
    q.enqueue(p) //re insert the key

    p = q.dequeue() 
    assert(p.key == "b")

    q.enqueue(p)//re insert the key
    p = q.dequeue()
    assert(p.key == "b")

    p = q.dequeue() 

    assert(p.key == "c")
    p.suspend()
    q.enqueue(p)

    p = q.dequeue()
    assert(p.key=="a")

    q.enqueue(new DevKey("z"))
    p = q.dequeue()
    assert(p.key == "z")

    p = q.dequeue()
    assert(p.key == "c")
    assert(p.maybeActivate()==false)
  }

  "A manage" should "manage" in {
    val m = new KeyManager(Seq(new DevKey("a"), new DevKey("b"), new DevKey("c")))
    assert(m.keyQueue.size==3)
    var k = m.getKey()
    assert(k=="a")
    k = m.getKey()
    assert(k=="a" )
    m.blockKey()
    assert(!m.currentKey.get.isActive)
    k = m.getKey()
    assert(m.currentKey.get.isActive)
    assert(k=="b")

    m.blockKey()
    m.getKey()
    m.blockKey()

    
    try {
      m.getKey()
    }catch {
      case e: KeyException => {
        assert(true)
      }
    }
    

  }
}


