package SparkExamples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Usage: BroadcastTest [partitions] [numElem] [blockSize]
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val blockSize = if (args.length > 2) args(2) else "4096"

    val conf = new SparkConf().setAppName("BroadcastTest")
      .setMaster("local[2]")
      .set("spark.broadcast.blockSize", blockSize)

    val sc = new SparkContext(conf)

    val partitions = if (args.length > 0) args(0).toInt else 2

    val num = if (args.length > 1) args(1).toInt else 10000

    val arr1 = (0 until num).toArray

    for (i <- 0 to 2) {
      println("Iteration" + i)
      println("============")
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)
      val observedSize = sc.parallelize(1 to 10, partitions).map(_ => barr1.value.length)
      observedSize.collect().foreach(println)
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))

    }

    sc.stop()

  }
}
