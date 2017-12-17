package ConvertTest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * (1, ("A"-> "a"))
  * (1, ("B"-> "b"))
  * (2, ("C"-> "c"))
  * (2, ("D"-> "d"))
  * (3, ("E"-> "e"))
  * (3, ("F"-> "f"))
  * 此类型数据集模拟一个schema为ID,KEY,VALUE的Table
  * 并对Table进行行转列，用List类型进行实现
  */

object ConvertTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConvertTest2").setMaster("local")
    val sc = new SparkContext(conf)

    val a = (1, ("A", "a"))
    val b = List((1, ("B", "b")), (2, ("C", "c")), (2, ("D", "d")), (3, ("E", "e")), (3, ("F", "f")))
    val c = a :: b
    //  println(c)

    val data = sc.parallelize(c)

    val checkList = List("A", "B", "C", "D", "E", "F")

    val listData = data.combineByKey(
      List(_)
      , (a: List[(String, String)], b: (String, String))
      => b :: a
      , (a: List[(String, String)], b: List[(String, String)]) => a ::: b
    )

    val defList = List("o")
    val diffRes = listData.map(p =>
      (p._1, checkList.diff(p._2.unzip._1)))
      .map(p =>
        (p._1, p._2.zipAll(defList, "*", "o")))

    val res = listData.join(diffRes).map(p => {
      (p._1, (p._2._1 ::: p._2._2).sorted)
    })


    res.map(row => {
      val resRow = row._1 + listToStr(row._2)
      resRow
    }
    ).collect().foreach(println)
  }

  def listToStr(listData: List[(String, String)]): String = {

    val str = new StringBuffer()
    listData.foreach(x => str.append(",").append(x._2))
    str.toString
  }

}
