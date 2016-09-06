package net.wujunwei.spark.java

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
  * Created by liushilin on 2016/3/15.
  */
object SparkTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("Test Load Model")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:/a.txt")

    case calss myTopic





    println(compact(render(json)))

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
