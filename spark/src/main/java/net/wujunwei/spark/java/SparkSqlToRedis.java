package net.wujunwei.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by wujunwei on 2016/8/30.
 */
public class SparkSqlToRedis {
    public static void main(String[] args) {
        System.out.println("hello");
    }

    /**
     * 简单的jedis连接测试
     */
    public static void jedisTest() {
        Jedis jedis = new Jedis("localhost");
//        jedis.auth("password");   //验证redis密码. 如果需要验证的话

        //验证简单的 set, get 命令, 其他命令类似
        String keyName = "foo";
        jedis.set(keyName, "hello redis");
        String value = jedis.get(keyName);
        System.out.println(value);
    }

    /**
     * jedis连接池操作测试
     */
    public static void jedisPoolTest() {

    }

    /**
     * spark sql 测试
     */
    public static void sparkSqlTest() {
        SparkConf conf=new SparkConf().setAppName("Spark-Hive-To-Redis").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
        DataFrame df = sqlContext.sql("SELECT * FROM table");
    }
}
