package SparkExamples
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Usage: GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]
  */
object GroupByTest {

  def main(args: Array[String]): Unit = {
    val numMappers=if (args.length>0) args(0).toInt else 2
    val numKVPairs=if (args.length>1) args(1).toInt else 1000
    val keySize=if (args.length>2) args(2).toInt else 1000
    val numReducers=if (args.length>3) args(3).toInt else numMappers
    val ranMax=numMappers*numKVPairs

    val conf=new SparkConf().setAppName("GroupByTest").setMaster("local")
    val sc=new SparkContext(conf)

    val pairs=sc.parallelize(0 until numMappers,numMappers).flatMap(p=>{
      val arr=new Array[(Int,Array[Byte])](numKVPairs)
      for (i<-0 until numKVPairs){
        val ranGen=new Random
        val byteArr=new Array[Byte](keySize)
        ranGen.nextBytes(byteArr)
        arr(i)=(ranGen.nextInt(ranMax),byteArr)
      }
      arr
    }).cache()

    pairs.count()
    println(pairs.groupByKey(numReducers).count)

  }

}
