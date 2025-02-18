package ai.chronon.orchestration.workflow

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

case class JedisConf(host: String = "localhost", port: Int = 6379, maxIdle: Int = 128, minIdle: Int = 16) {
  def createPool(): JedisPool = {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxIdle(128)
    poolConfig.setMinIdle(16)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)
    poolConfig.setNumTestsPerEvictionRun(3)
    poolConfig.setBlockWhenExhausted(true)
    new JedisPool(poolConfig, host, port)
  }
}