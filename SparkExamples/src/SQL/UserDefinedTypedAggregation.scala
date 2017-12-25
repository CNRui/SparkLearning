package SQL

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

object UserDefinedTypedAggregation {

  object MyAverage extends Aggregator[Employee, Average, Double] {

    def zero: Average = Average(0L, 0L)

    def reduce(b: Average, a: Employee): Average = {
      b.sum += a.salary
      b.count += 1
      b
    }

    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    def finish(reduction: Average): Double = {
      reduction.sum.toDouble / reduction.count
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserDefinedTypedAggregation").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    import sqlC.implicits._
    val ds = sqlC.read.json("file:///E:/github/SparkExamples/resources/employees.json").as[Employee]
    ds.show()

    val averageSalary = MyAverage.toColumn
    val result = ds.select(averageSalary)
    result.show()

    //DF 无法做聚合
    //val result2=ds.toDF().select(averageSalary)
    //result2.show()


    sc.stop()

  }

}
