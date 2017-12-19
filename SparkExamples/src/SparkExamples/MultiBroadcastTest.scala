package SparkExamples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Usage: MultiBroadcastTest [partitions] [numElem]
  */
object MultiBroadcastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MultiBroadcastTest").setMaster("local")
    val sc = new SparkContext(conf)

    val partitions = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000

    val arr1 = (0 until num).toArray
    val arr2 = (0 until num).toArray

    val bArr1 = sc.broadcast(arr1)
    val bArr2 = sc.broadcast(arr2)

    val observedSize = sc.parallelize(0 until 10, partitions).map(p =>
      (bArr1.value.length, bArr2.value.length)
    )

    for (elem <- observedSize.collect()) {
      println(elem)
    }

  }
}
