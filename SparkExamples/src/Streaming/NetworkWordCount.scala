package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      //System.err.println("Usage: NetworkWordCount <hostname> <port>")
      //System.exit(1)
      println("Usage: NetworkWordCount <hostname> <port>")
    }
    //nc -lk 9998

    val host=if (args.length>0) args(0) else "d2hadoop05"
    val port=if (args.length>1) args(1).toInt else 9998

    val conf =new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    val words=lines.flatMap(_.split(" "))
    words.map(p=>(p,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }

}
