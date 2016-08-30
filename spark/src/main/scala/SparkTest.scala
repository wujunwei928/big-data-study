

/**
  * Created by liushilin on 2016/3/15.
  */
object SparkTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test Load Model").setMaster("local")

    val sc = new SparkContext(conf)

    sc.stop()
  }

}
