package SparkExamples

import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random

object HdfsTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:HdfsTest <file>")
      System.exit(0)
    }

    val conf = new SparkConf().setAppName("HdfsTest").setMaster("local")
    val sc = new SparkContext(conf)

    val fileData = sc.textFile(args(0))
    val mapData = fileData.map(_.length).cache()
    mapData.foreach(println)
    for (i <- 0 until 10) {
      val startTime = System.currentTimeMillis()
      mapData.collect().foreach(println)
      val endTime = System.currentTimeMillis()

      println("Iteration: " + i + " take " + (endTime - startTime).toString + " ms ")
    }
    sc.stop()
  }

}
