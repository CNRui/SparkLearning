package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      //System.err.println("Usage: HdfsWordCount <directory>")
      //System.exit(1)
      println("Usage: HdfsWordCount <directory>")
    }

    val path = if (args.length > 0) args(0) else "hdfs://10.108.240.12:8020/test/zhanrui/"

    val conf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream(path)
    val words = lines.flatMap(_.split(" "))
    words.map(p => (p, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }

}
