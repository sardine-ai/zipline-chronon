package ai.chronon.orchestration.workflow

import redis.clients.jedis.JedisPool

case class RunContext(jedisConf: JedisConf) {
  val jedisPool: JedisPool = jedisConf.createPool()
}
