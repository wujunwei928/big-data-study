package net.wujunwei.spark.java

import org.apache.spark.{SparkConf, SparkContext}
import org.datanucleus.store.types.simple.HashMap
import redis.clients.jedis.Jedis
import org.json4s.JsonDSL._
import org.json4s.native.Json
import org.json4s.DefaultFormats



/**
  * spark读取文件，将文件信息汇总成json，然后存入redis
  *
  * @author Wujunwei
  * @email  1399952803@qq.com
  * @github https://github.com/wujunwei928
  * @blog   http://www.wujunwei.net
  *
  */
object SparkTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("mapConvertJsonToRedis")

    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/wujunwei/study/a.txt")

    val tmpData1 = data.map(line => line.split(1.toChar.toString))

    //通过取唯一值，获取可能出现的topic_form
    //注意这里一定要先用collect取到一个数组，下面再用foreach循环，否则RDD循环嵌套会报错
    val topic_form_list = tmpData1.map(line=>line(0)).distinct.collect()

    //设置一个class，用来存储topic
    case class My_Topic(id: Long, name: String)

    //拼接生成json的map
    var jsonMap:Map[String,Any] = Map()    //定义一个空的map
    topic_form_list.foreach(topic_form => {
      //根据topic_form过滤后，将相同topic_form的topic汇总在一起
      val tmpTopics = tmpData1.filter(line => line(0) == topic_form).map(
        line => My_Topic(line(2).toLong, line(1))
      ).collect()

      //map拼接新元素
      jsonMap += ("topic_form_" + topic_form ->
        Map("topics" -> tmpTopics, "topicCount" -> tmpTopics.length))
    })

    //将Map生成json
    val json = Json(DefaultFormats).write(jsonMap)
//    println(json)

    
    //链接redis
    val redisHost = "127.0.0.1"     //redis地址
    val redisPort = 6379            //redis端口
    val redisClient = new Jedis(redisHost, redisPort)
//    redisClient.auth("redisPassword")       //验证redis密码，如果需要的话

    //设置redis缓存
    val topic_form_redis_key = "phoenixindex_newslink"
    redisClient.set(topic_form_redis_key, json)
    println(redisClient.get(topic_form_redis_key))
  }

}
