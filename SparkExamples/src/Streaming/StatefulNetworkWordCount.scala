package Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

object StatefulNetworkWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    if (args.length < 2) {
      System.err.println("Usage: StatefulNetworkWordCount <hostname> <port>")
      //System.exit(1)
    }
    val host = if (args.length > 0) args(0) else "d2hadoop05"
    val port = if (args.length > 1) args(1).toInt else 9998

    val conf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://10.108.240.12:8020/test/check/")

    val initialRDD=ssc.sparkContext.parallelize(List(("hello",1),("world",1)))
    val lines=ssc.socketTextStream(host,port,StorageLevel.MEMORY_ONLY)
    val words=lines.flatMap(_.split(" "))
    val wordDstream=words.map(p=>(p,1))

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      if(state.isTimingOut()){
        System.out.println(word+" is timeout")
      }
      else {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
      }
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD).timeout(Seconds(20)))
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
