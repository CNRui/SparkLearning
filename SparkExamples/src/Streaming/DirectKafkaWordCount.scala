package Streaming

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils


object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      //      System.err.println(s"""
      //                            |Usage: DirectKafkaWordCount <brokers> <topics>
      //                            |  <brokers> is a list of one or more Kafka brokers
      //                            |  <topics> is a list of one or more kafka topics to consume from
      //                            |
      //        """.stripMargin)
      //      System.exit(1)
      println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
    }

    val brokers = if (args.length > 0) args(0) else "10.108.240.14:9092,10.108.240.12:9092,10.108.240.13:9092,10.108.240.15:9092"
    val topics = if (args.length > 1) args(1) else "TESTZR"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val conf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //    val kafkaOps = Map[String, String](
    //      "metadata.broker.list" -> "10.108.240.14:9092,10.108.240.12:9092,10.108.240.13:9092,10.108.240.15:9092",
    //      "auto.offset.reset" -> "smallest",
    //      "enable.out.commit" -> "false"
    //    )


    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)

    lines.flatMap(_.split(" ")).map(p => (p, 1L)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
