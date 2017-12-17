package ConvertTest

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

/**
  * (1, ("A"-> "a"))
  * (1, ("B"-> "b"))
  * (2, ("C"-> "c"))
  * (2, ("D"-> "d"))
  * (3, ("E"-> "e"))
  * (3, ("F"-> "f"))
  * 此类型数据集模拟一个schema为ID,KEY,VALUE的Table
  * 并对Table进行行转列，用Map类型进行实现
  */

object ConvertTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConvertTest").setMaster("local")
    val sc = new SparkContext(conf)

    val a = (1, ("A", "a"))
    val b = List((1, ("B", "b")), (2, ("C", "c")), (2, ("D", "d")), (3, ("E", "e")), (3, ("F", "f")))
    val c = a :: b

    val mapData = sc.parallelize(c)

    val checkMap = SortedMap("A" -> "U", "B" -> "U", "C" -> "U", "D" -> "U", "E" -> "U", "F" -> "U")

    val newMap = mapData.map(p => (p._1, Map(p._2._1 -> p._2._2)))

    val res = newMap.reduceByKey((a, b) => a ++ b)

    //相当于把m1中的元素加入到m2中，如果有相同的就用m1中的元素覆盖m2中的
    //val r1=m1.++:(m2)

    //相当于把m2中的元素加入到m1中，如果有重复的用m2的覆盖m1的
    //val r2 = m1.++(m2)

    res.map(p => {
      (p._1, checkMap.++(p._2))
    }).map(p => {
      p._1 + mapToStr(p._2)
    }).collect().foreach(println)

  }

  def mapToStr(map: SortedMap[String, String]): String = {
    val str = new StringBuffer()
    map.foreach(x => str.append(",").append(x._2))
    str.toString
  }
}
