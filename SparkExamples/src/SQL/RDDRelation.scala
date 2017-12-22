package SQL

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object RDDRelation {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    import hc.implicits._

    val df = hc.createDataFrame((0 to 100).map(i => Record(i, s"val_$i")))

    df.registerTempTable("records")

    println("Result of SELECT:")
    hc.sql("select * from records limit 10").show()

    println("Result of COUNT:")
    hc.sql("select count(*) from records").show()

    val rddFromSql = hc.sql("select * from records where key<15")
    println("Result of RDD:")
    rddFromSql.rdd.map(row => s"key:${row(0)},value:${row(1)}").collect.foreach(println)

    df.write.mode(SaveMode.Overwrite).parquet("hdfs://mycluster/test/pair.parquet")

    val parquetFile = hc.read.parquet("hdfs://mycluster/test/pair.parquet")

    parquetFile.where($"key" === 1).select($"value".as("a")).collect.foreach(println)

    sc.stop()
  }

}
