package SparkExamples

import java.util.Random
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Usage: SimpleSkewedGroupByTest [numMappers] [numKVPairs] [valSize] [numReducers] [ratio]
  */
object SimpleSkewedGroupByTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleSkewedGroupByTest").setMaster("local")
    val sc = new SparkContext(conf)

    val numMappers = if (args.length > 0) args(0).toInt else 2
    val numKVPaires = if (args.length > 1) args(1).toInt else 1000
    val size = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers
    val ratio = if (args.length > 4) args(4).toInt else 5.0

    val pairs = sc.parallelize(0 until numMappers, numMappers).flatMap(p => {
      val ranGen = new Random
      val arr = new Array[(Int, Array[Byte])](numKVPaires)
      for (i <- 0 until numKVPaires) {
        val byteArr = new Array[Byte](size)
        ranGen.nextBytes(byteArr)
        val offset = ranGen.nextInt(1000) * numReducers
        if (ranGen.nextInt(10) < 8)
          arr(i) = (offset, byteArr)
        else {
          val key = 1
          arr(i) = (key, byteArr)
        }
      }
      arr
    }).cache()

    pairs.count()



    println(pairs.groupByKey(numReducers).count())
    // Print how many keys each reducer got (for debugging)
    val mapRes=pairs.groupByKey(numReducers)
      .map { case (k, v) => (k, v.size) }
      .collectAsMap
    println("RESULT: " + mapRes)
    //ranGen.nextInt(10) < 8 :key=1的数量为numMappers*numKVPairs*0.2
    println(mapRes(1))

    sc.stop()
  }

}
