

/**
  * Created by liushilin on 2016/3/15.
  */
object SparkTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("Test Load Model")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("~/test.log")
    val pythonLines = lines.filter(lambda line: "Python" in line)
    println("----output----------------------")
    println(pythonLines.first())

    sc.stop()
  }

}
