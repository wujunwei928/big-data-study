package net.wujunwei.spark.java;

import redis.clients.jedis.Jedis;

/**
 * Created by wujunwei on 2016/8/30.
 */
public class SparkSqlToRedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost");

        String keyName = "foo";
        System.out.println(jedis.get(keyName));
        jedis.set(keyName, "hello redis");
        String value = jedis.get(keyName);
        System.out.println(value);
    }
}
