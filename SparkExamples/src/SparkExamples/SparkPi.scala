package SparkExamples

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random
import scala.util.Random

object SparkPi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkPi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val partitions = if (args.length > 0) args(0).toInt else 2
    val num = math.min(1000000L * partitions, Int.MaxValue).toInt

    val count = sc.parallelize(0 until num, partitions).map(f = p => {
      val ranGen = new Random
      val x = ranGen.nextDouble()
      val y = ranGen.nextDouble()
      //      val x = random * 2 - 1
      //      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }).reduce(_ + _)

    println("Pi is roughly :" + 4.0 * count / (num - 1))
    sc.stop()
  }

}
