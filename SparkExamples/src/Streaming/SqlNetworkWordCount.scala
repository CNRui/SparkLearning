package Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

case class Record(words: String)

object SqlNetworkWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      //System.exit(1)
    }
    val host = if (args.length > 0) args(0) else "d2hadoop05"
    val port = if (args.length > 1) args(1).toInt else 9998

    val conf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_ONLY)
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      val sqlC = new SQLContext(ssc.sparkContext)
      import sqlC.implicits._
      val word = rdd.map(w => Record(w)).toDF()
      word.registerTempTable("wordTable")
      sqlC.sql("select words, count(*) as total from wordTable group by words").show()
      println(s"========= $time =========")
      word.show()
    }
    ssc.start()
    ssc.awaitTermination()

    //nc -lk 9998 输入 aa aa aa 11 11 1 输出结果：
    //      +-----+-----+
    //      |words|total|
    //      +-----+-----+
    //      |   aa|    3|
    //      |    1|    1|
    //      |   11|    2|
    //      +-----+-----+
    //
    //    ========= 1514431280000 ms =========
    //      +-----+
    //      |words|
    //      +-----+
    //      |   aa|
    //      |   aa|
    //      |   aa|
    //      |   11|
    //      |   11|
    //      |    1|
    //      +-----+

  }

}
