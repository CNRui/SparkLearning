package SQL

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkHiveExample {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkHiveExample").setMaster("local")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    import hc.implicits._
    import hc.sql

    //   sql("CREATE TABLE IF NOT EXISTS test.src (key INT, value STRING)")
    //   sql("LOAD DATA LOCAL INPATH 'E:/github/SparkExamples/resources/kv1.txt' INTO TABLE test.src")
    sql("show databases").foreach(println)
    //    sql("SELECT * FROM test.src").show()
    //
    //    sql("SELECT COUNT(*) FROM test.src").show()
    //
    //
    //    val sqlDF = sql("SELECT key, value FROM test.src WHERE key < 10 ORDER BY key")
    //
    //
    //
    //    val stringsDS = sqlDF.map {
    //      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    //    }
    //
    //
    //    stringsDS.toDF().show()
    //
    //    val recordsDF = hc.createDataFrame((1 to 100).map(i => Record(i, s"var_$i")))
    //
    //    recordsDF.registerTempTable("records")
    //
    //    sql("SELECT r.* FROM records r JOIN test.src s ON r.key = s.key").show()


    sc.stop()

  }

}
