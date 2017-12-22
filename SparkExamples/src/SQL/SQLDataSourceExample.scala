package SQL


import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLDataSourceExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDataSourceExample").setMaster("local")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    runBasicDataSourceExample(hc)
    //runBasicParquetExample(hc)

    sc.stop()
  }

  def runBasicDataSourceExample(hiveContext: HiveContext): Unit = {
    val userDF = hiveContext.read.load("file:///E:/github/SparkExamples/resources/users.parquet")
    userDF.show()
    //   userDF.select("name", "favorite_color").write.save("file:///E:/fileT/namesAndFavColors.parquet")

    val peopleDF = hiveContext.read.format("json").load("file:///E:/github/SparkExamples/resources/people.json")
    peopleDF.show()
    //  peopleDF.select("name","age").write.format("parquet").save("file:///E:/fileT/namesAndAges.parquet")

    //spark1.6 不支持csv
    /* val peopleDFCsv=hiveContext.read.format("csv")
       .option("sep",";")
       .option("inferSchema","true")
       .option("header","true")
       .load("file:///E:/github/SparkExamples/resources/people.csv")
     peopleDFCsv.show()*/

    val sqlDF = hiveContext.sql("SELECT * FROM parquet.`file:///E:/github/SparkExamples/resources/users.parquet`")
    sqlDF.show()

    //spark1.6不支持写分桶
    //peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

    //userDF.write.partitionBy("favorite_color").format("parquet").save("file:///E:/fileT/namesPartByColor.parquet")


    peopleDF
      .write
      .partitionBy("favorite_color")
      .saveAsTable("people_partitioned_bucketed")

    //    userDF
    //      .write
    //      .partitionBy("favorite_color")
    //      .saveAsTable("test.user_partitioned")

    //peopleDF.schema.foreach(println)

  }

  private def runBasicParquetExample(hiveContext: HiveContext): Unit = {
    import hiveContext.implicits._
    val peopleDF = hiveContext.read.json("file:///E:/github/SparkExamples/resources/people.json")
    peopleDF.write.parquet("file:///E:/fileT/people.parquet")

    val parquetPeopleDF = hiveContext.read.parquet("file:///E:/fileT/people.parquet")
    parquetPeopleDF.registerTempTable("people")

    val namesDF = hiveContext.sql("select name from people where age < 20")
    namesDF.map(row => s"name is:${row(0)}").foreach(println)
  }

}
