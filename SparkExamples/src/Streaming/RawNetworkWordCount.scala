package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

object RawNetworkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      //System.err.println("Usage: NetworkWordCount <hostname> <port>")
      //System.exit(1)
      println("Usage: RawNetworkWordCount <numStreams> <host> <port>")
    }
    //nc -lk 9998


    val numStreams = if (args.length > 0) args(0).toInt else 3
    val host = if (args.length > 1) args(1) else "d2hadoop05"
    val port = if (args.length > 2) args(2).toInt else 9998

    val conf = new SparkConf().setAppName("RawNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val rawStream = (1 to numStreams).map(_ =>
      ssc.rawSocketStream[String](host, port, StorageLevel.MEMORY_AND_DISK_SER)
    ).toArray

    val union = ssc.union(rawStream)
    //union.flatMap(_.split(" ")).map(p => (p, 1)).reduceByKey(_ + _).print()
    union.filter(_.contains("the")).count().foreachRDD(r =>
      println("Grep count: " + r.collect().mkString))
    ssc.start()
    ssc.awaitTermination()
    //执行异常java.lang.OutOfMemoryError: Java heap space
  }

}
