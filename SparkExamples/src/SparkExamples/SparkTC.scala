package SparkExamples

import scala.collection.mutable
import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}

object SparkTC {
  val numEdges = 200
  val numVector = 100
  val ranGen = new Random

  def generateGraph: Seq[(Int, Int)] = {
    val set: mutable.Set[(Int, Int)] = mutable.Set.empty
    while (set.size < numEdges) {
      val x = ranGen.nextInt(numVector)
      val y = ranGen.nextInt(numVector)
      if (x != y) set.+=((x, y))
    }
    set.toSeq
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTC").setMaster("local")
    val sc = new SparkContext(conf)

    var lines = sc.parallelize(generateGraph)
    val transLines = lines.map(x => (x._2, x._1))

    var oldCount = 0
    var newCount = numEdges
    do {
      oldCount = newCount
      lines = lines.union(lines.join(transLines).map(x => (x._2._2, x._2._1))).distinct()
      newCount = transLines.count.toInt
    } while (oldCount != newCount)

    println("SparkTC has" + lines.count())
    sc.stop()
  }

}
