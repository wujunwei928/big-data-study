package net.wujunwei.spark.java

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by liushilin on 2016/3/15.
  */
object SparkTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("Test Load Model")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("/data/spark/README.md")
    val pythonLines = lines.filter(line => line.contains("Python"))
    println("----output----------------------")
    println(pythonLines.first())

    sc.stop()
  }

}
