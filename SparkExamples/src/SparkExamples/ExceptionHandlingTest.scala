package SparkExamples

import org.apache.spark.{SparkConf, SparkContext}

object ExceptionHandlingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExceptionHandlingTest").setMaster("local")
    val sc = new SparkContext(conf)

    sc.parallelize(0 until sc.defaultParallelism).foreach(i =>
      if (math.random > 0.75) {
        throw new Exception("Testing ExceptionHandling")
      }
    )
    sc.stop()
  }

}
