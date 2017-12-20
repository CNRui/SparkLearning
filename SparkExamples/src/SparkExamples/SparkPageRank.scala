package SparkExamples

import org.apache.spark.{SparkConf, SparkContext}

object SparkPageRank {
  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    /*    if (args.length < 1) {
          System.err.println("Usage: SparkPageRank <file> <iter>")
          System.exit(1)
        }*/

    showWarning()

    val file = if (args.length > 0) args(0) else "E:\\fileT\\pagerank_data.txt"
    val iter = if (args.length > 1) args(1).toInt else 10

    val conf = new SparkConf().setAppName("SparkPageRank").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(file)

    val links = lines.map(_.split("\\s+")).map(s => (s(0), s(1))).distinct().groupByKey().cache()

    var ranks = links.mapValues(p => 1.0)

    // for (i <- 0 until iter) {
    val contribs = links.join(ranks).values.flatMap {
      case (urls, rank) => {
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
    }
    contribs.collect().foreach(println)
    //    (1,1.0)
    //    (3,0.3333333333333333)
    //    (4,0.3333333333333333)
    //    (2,0.3333333333333333)
    //    (1,1.0)
    //    (1,1.0)

    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    // }
    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    sc.stop()

    //    4 has rank: 0.43333333333333335.
    //    2 has rank: 0.43333333333333335.
    //    3 has rank: 0.43333333333333335.
    //    1 has rank: 2.6999999999999997.

  }

}
