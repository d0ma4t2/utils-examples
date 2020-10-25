package cn.colorfulboxes.utils.spark

import java.time.Duration

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands


object RedisUtil {

    private val client: RedisClient = RedisClient.create("redis://localhost")
    // connection, 线程安全的长连接，连接丢失时会自动重连，直到调用 close 关闭连接。
    private val connection: StatefulRedisConnection[String, String] = client.connect

    /**
     * 直接得到一个 Redis 的连接
     */
    def redisCommands: RedisCommands[String, String] = {
        // sync, 设置超时时间30s.
        connection.setTimeout(Duration.ofSeconds(30))
        connection.sync
    }

    /**
     * 关闭redis
     */
    def close = {
        connection.close
        client.shutdown
    }

}