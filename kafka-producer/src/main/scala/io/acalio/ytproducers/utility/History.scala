package io.acalio.ytproducers.utility

import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import java.{util => ju}
import redis.clients.jedis.Jedis
/**
  * This class defines the mechanism for 
  * storing and retrieven data from a redis-based cache system
  * 
  * It requires the information to connect to a redis isntance (or cluster)
  * The Key/Vaue pairs stored into this cache have the following structure: 
  *  <topic-name>-<entity> {id1, ......idN}
  * 
  * Basically, the redis cache keeps track of every resource that has been sent to the kafka topics, so it 
  * does not need to be downloaded from the youtube api - so we can save some quota. 
  *  
  * 
  */
class History(
 val hostAndPorts: ju.Set[HostAndPort] 
){

  // val jedisCluster : JedisCluster = new JedisCluster(hostAndPorts)
  // val jedisCluster: Jedis = new Jedis("localhost")
  val jedis: Jedis = new Jedis("localhost")
  def add(key: String, resourceId: String): Long= {
    // return jedisCluster.sadd(key, resourceId)
    return jedis.sadd(key, resourceId)
  }

  def add(key: String, resourceIds: Seq[String]): Long = {
    return jedis.sadd(key,resourceIds: _*)
  }

  def isPresent(key: String, resourceId: String): Boolean = {
    return jedis.sismember(key, resourceId)
  }

}


