package net.wujunwei.spark.java

import org.apache.spark.{SparkConf, SparkContext}
import org.datanucleus.store.types.simple.HashMap
import redis.clients.jedis.Jedis
import org.json4s.JsonDSL._
import org.json4s.native.Json
import org.json4s.DefaultFormats



/**
  * Created by liushilin on 2016/3/15.
  */
object SparkTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("mapConvertJsonToRedis")
//
    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/wujunwei/study/a.txt")

    val tmpData1 = data.map(line => line.split(1.toChar.toString))

    val topic_form_list = tmpData1.map(line=>line(0)).distinct.collect()

    case class My_Topic(id: String, name: String)

    var jsonMap:Map[String,Any] = Map()
    topic_form_list.foreach(topic_form => {
      val tmpTopics = tmpData1.filter(line => line(0) == topic_form).map(
        line => My_Topic(line(2), line(1))
      ).collect()

      jsonMap += ("topic_form_" + topic_form ->
        Map("topics" -> tmpTopics, "topicCount" -> tmpTopics.length))
    })

    val json = Map(
      "topic_form_0" -> Map(
        "topics" -> List(
          Map("id" -> 1781, "name" -> "wenju") ,
          Map("id" -> 1781, "name" -> "wenju")
        ),
        "count" -> 2
      ),
      "topic_form_1" -> Map(
        "topics" -> List(
          Map("id" -> "1781", "name" -> "wenju") ,
          Map("id" -> "1781", "name" -> "wenju")
        ),
        "count" -> 2
      )
    )

    println(Json(DefaultFormats).write(jsonMap))
//    println(compact(render(json)))


  }

  def test(): Unit = {
    case class Winner(id: Long, numbers: List[Int])
    case class Lotto(id: Long, winningNumbers: List[Int], winners: List[Winner], drawDate: Option[java.util.Date])

    val winners = List(Winner(23, List(2, 45, 34, 23, 3, 5)), Winner(54, List(52, 3, 12, 11, 18, 22)))
    val lotto = Lotto(5, List(2, 45, 34, 23, 7, 5, 3), winners, None)

    val json =
      ("lotto" ->
        ("lotto-id" -> lotto.id) ~
          ("winning-numbers" -> lotto.winningNumbers) ~
          ("draw-date" -> lotto.drawDate.map(_.toString)) ~
          ("winners" ->
            lotto.winners.map { w =>
              (("winner-id" -> w.id) ~
                ("numbers" -> w.numbers))}))
  }

  def spark_test(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test Load Model")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:/Big_Data/spark-1.6.1-bin-hadoop2.6/README.md")
    val pythonLines = lines.filter(line => line.contains("Python"))
    println("----output----------------------")
    println(pythonLines.first())

    sc.stop()
  }

  def redis_test(): Unit = {
    val redisHost = "192.168.2.106"
    val redisPort = 6379
    val redisClient = new Jedis(redisHost, redisPort)
//    redisClient.auth(redisPassword)

    var result = redisClient.get("a");
    println(result)
  }

}
