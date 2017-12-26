package Streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


object CustomReceiver {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      //System.err.println("Usage: CustomReceiver <hostname> <port>")
      //System.exit(1)
      println("Usage: CustomReceiver <hostname> <port>")
    }


    val conf = new SparkConf().setAppName("CustomReceiver").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val host = if (args.length > 0) args(0) else "d2hadoop05"
    val port = if (args.length > 1) args(1).toInt else 9998
    val lines = ssc.receiverStream(new CustomReceiver(host, port))
    val words = lines.flatMap(_.split(","))
    val res = words.map(p => (p, 1)).reduceByKey(_ + _)
    res.print()
    ssc.start()
    ssc.awaitTermination()


    //    在d2hadoop05输入：nc -lk 9998
    //    dd,c,g
    //    输出结果：
    //    -------------------------------------------
    //    Time: 1514275432000 ms
    //      -------------------------------------------
    //    (dd,1)
    //    (g,1)
    //    (c,1)
  }
}

class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  private def receive(): Unit = {
    var socket: Socket = null
    var userInput: String = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connected to " + host + ":" + port)
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while (!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }


  }
}


