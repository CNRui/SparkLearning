package Streaming

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


object WordBlacklist {

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

object DroppedWordsCounter {

  @volatile private var instance: Accumulator[Int] = null

  def getInstance(sc: SparkContext): Accumulator[Int] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0, "Example Accumulator")
        }
      }
    }
    instance
  }
}

object RecoverableNetworkWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    if (args.length != 4) {
      System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
          |     <output-file>. <hostname> and <port> describe the TCP server that Spark
          |     Streaming would connect to receive data. <checkpoint-directory> directory to
          |     HDFS-compatible file system which checkpoint data <output-file> file to which the
          |     word counts will be appended
          |
          |In local mode, <master> should be 'local[n]' with n > 1
          |Both <checkpoint-directory> and <output-file> must be absolute paths
        """.stripMargin
      )
      //System.exit(1)
    }
    //nc -lk 9998
    val host= if (args.length > 0) args(0) else "d2hadoop05"
    val port = if (args.length > 1) args(1).toInt else 9998
    val checkpointDir=if (args.length > 2) args(2) else "hdfs://10.108.240.12:8020/test/check/"
    val outputPath=if (args.length > 3) args(3) else "E:/github/SparkExamples/resources/out.txt"

    val conf=new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[2]")
    val ssc=StreamingContext.getOrCreate(checkpointDir,()=>new StreamingContext(conf,Seconds(5)))


    val outputFile=new File(outputPath)
    if (outputFile.exists()) outputFile.delete
    ssc.checkpoint(checkpointDir)

    val lines=ssc.socketTextStream(host,port)
    val words=lines.flatMap(_.split(" "))
    val wordCount=words.map(p=>(p,1)).reduceByKey(_+_)

    wordCount.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
      val counts = rdd.filter { case (word, count) =>
        if (blacklist.value.contains(word)) {
          droppedWordsCounter.add(count)
          false
        } else {
          true
        }
      }.collect().mkString("[", ", ", "]")
      val output = "Counts at time " + time + " " + counts
      println(output)
      println("Dropped " + droppedWordsCounter.value + " word(s) totally")
      println("Appending to " + outputFile)
      Files.append(output + "\n", outputFile, Charset.defaultCharset())
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
