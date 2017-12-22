package SQL

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLExample").setMaster("local")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlc = new SQLContext(sc)

    //runBasicDataFrameExample(hc)
    //runDatasetCreationExample(hc)
    //runInferSchemaExample(sqlc)
    runProgrammaticSchemaExample(sqlc)

  }


  private def runBasicDataFrameExample(hiveContext: HiveContext): Unit = {
    val path = "file:///E:/github/SparkExamples/resources/people.json"
    val df = hiveContext.read.json(path)

    df.show()

    import hiveContext.implicits._
    df.printSchema()

    df.select("name").show()

    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 21).show()

    df.groupBy("age").count().show()

    df.registerTempTable("people")

    val sqlDF = hiveContext.sql("select * from people")
    sqlDF.show()

    //spark1.6 没有createGlobalTempView
    //Global temporary view is cross-session

  }

  private def runDatasetCreationExample(sQLContext: SQLContext): Unit = {

    //spark1.6 报错
    import sQLContext.implicits._

    //  val caseClassDS = Seq(Person("Andy", 32)).toDS()
    //  caseClassDS.show()

    //    val primitiveDS = Seq(1, 2, 3).toDS()
    //    primitiveDS.map(_ + 1).collect()

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    //    val path = "file:///E:/github/SparkExamples/resources/people.json"
    //    val ds = sQLContext.read.json(path).as[Person]
    //    ds.show()

  }

  private def runInferSchemaExample(sQLContext: SQLContext):Unit={

    import sQLContext.implicits._

    val peopleDF = sQLContext.sparkContext
      .textFile("file:///E:/github/SparkExamples/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.registerTempTable("people")

    val teenagersDF = sQLContext.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    teenagersDF.show()

    teenagersDF.map(teenager => "Name: " + teenager(0)).toDF().show()
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).toDF().show()

    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  }

  private def runProgrammaticSchemaExample(sQLContext: SQLContext):Unit={
    import sQLContext.implicits._
    val peopleRDD=sQLContext.sparkContext.textFile("file:///E:/github/SparkExamples/resources/people.txt")
    val schemaString="name age"

    val fields=schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,nullable = true))
    val schema=StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = sQLContext.createDataFrame(rowRDD, schema)

    peopleDF.registerTempTable("people")

    val results = sQLContext.sql("SELECT name FROM people")

    results.map(attributes => "Name: " + attributes(0)).toDF().show()

  }

}
