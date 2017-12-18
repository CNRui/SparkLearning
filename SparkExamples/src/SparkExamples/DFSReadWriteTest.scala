package SparkExamples

import java.io.File

import scala.io.Source._

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple test for reading and writing to a distributed
  * file system.  This example does the following:
  *
  *   1. Reads local file
  *   2. Computes word count on local file
  *   3. Writes local file to a DFS
  *   4. Reads the file back from the DFS
  *   5. Computes word count on the file using Spark
  *   6. Compares the word count results
  */
object DFSReadWriteTest {

  private var localFilePath: File = new File(".")
  private var dfsDirPath: String = ""

  private val NPARAMS = 2

  private def readFile(fileName: String): List[String] = {
    val iterLines: Iterator[String] = fromFile(fileName).getLines()
    val listLines: List[String] = iterLines.toList
    listLines
  }

  private def printUsage(): Unit = {
    val usage: String = "DFS Read-Write Test\n" +
      "\n" +
      "Usage: localFile dfsDir\n" +
      "\n" +
      "localFile - (string) local file to use in test\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"

    println(usage)
  }

  private def parsArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    var i = 0
    localFilePath = new File(args(i))
    if (!localFilePath.exists) {
      System.err.println("File path:" + args(i) + "not exist!")
      printUsage()
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println("File:" + args(i) + "not exist!")
      printUsage()
      System.exit(1)
    }
    i += 1
    dfsDirPath = args(i)
  }

  def runLocalWordCount(fileContent: List[String]): Int = {
    fileContent.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }

  def main(args: Array[String]): Unit = {
    parsArgs(args)

    println("Performing local word count")
    val fileContents = readFile(localFilePath.toString)
    val localWordCount = runLocalWordCount(fileContents)

    println("creating SparkContext")
    val conf = new SparkConf().setAppName("DFSReadWriteTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    println("Performing HDFS word count")
    println("Write to HDFS")
    dfsDirPath = dfsDirPath + "/read_write_test"

    val fileRDD = sc.parallelize(fileContents)
    fileRDD.saveAsTextFile(dfsDirPath)

    println("Read from HDFS")
    val readRDD = sc.textFile(dfsDirPath)
    val dfsWordCount = readRDD.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w, 1)).countByKey().values.sum

    sc.stop()

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) agree.")
    } else {
      println(s"Failure! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) disagree.")
    }

  }

}
