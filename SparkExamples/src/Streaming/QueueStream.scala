package Streaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

object QueueStream {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val rddQueue = new Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(p => (p % 10, 1))
    mappedStream.reduceByKey(_ + _).print()
    ssc.start()

    for (i <- 1 to 10) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to i*100, 10)
      }
      Thread.sleep(1000)
    }

    ssc.stop()
  }

}
