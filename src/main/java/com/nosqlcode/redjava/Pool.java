package com.nosqlcode.redjava;

/**
 * Project: redjava
 * User: thomassilva
 * Date: 8/13/13
 */

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Pool {

    public static JedisPool pool;

    public static void connect(String ipAddress, int port) {
        // Create and set a JedisPoolConfig
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // Maximum active connections to Redis instance
        poolConfig.setMaxTotal(20);
        // Tests whether connection is dead when connection
        // retrieval method is called
        poolConfig.setTestOnBorrow(true);
        /* Some extra configuration */
        // Tests whether connection is dead when returning a
        // connection to the pool
        poolConfig.setTestOnReturn(true);
        // Number of connections to Redis that just sit there
        // and do nothing
        poolConfig.setMaxIdle(5);
        // Minimum number of idle connections to Redis
        // These can be seen as always open and ready to serve
        poolConfig.setMinIdle(1);
        // Tests whether connections are dead during idle periods
        poolConfig.setTestWhileIdle(true);
        // Maximum number of connections to test in each idle check
        poolConfig.setNumTestsPerEvictionRun(10);
        // Idle connection checking period
        poolConfig.setTimeBetweenEvictionRunsMillis(60000);
        // Create the jedisPool
        pool = new JedisPool(poolConfig, ipAddress, port);
    }
    public static void release() {
        pool.destroy();
    }
    public static Jedis getJedis() {
        return pool.getResource();
    }
    public static void returnJedis(Jedis jedis) {
        pool.returnResource(jedis);
    }
}