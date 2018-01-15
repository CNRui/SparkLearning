package SparkML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
  * 基于RDD的RandomForestClassification
  */
object RandomForestClassification {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("RandomForestClassificationExample").setMaster("local")
    val sc = new SparkContext(conf)

    val data = loadFile(sc, "E:\\文档\\lc_bubble case\\glass_test\\lc_bubble_spark.txt", 1138, 2)

    // 拆分训练集测试集
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // 训练随机森林模型
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 500 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // 开始模型评估
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics1 = new MulticlassMetrics(scoreAndLabels)

    // 混淆矩阵 输入：（预测值，实际值）
    println("Confusion matrix:")
    println(metrics1.confusionMatrix)

    // AUC和PRC值
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    val auPRC = metrics.areaUnderPR()

    println("Area under ROC = " + auROC)
    println("Area under PRC = " + auPRC)

    // 错误率
    val testErr = scoreAndLabels.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")

    // println(s"Learned classification forest model:\n ${model.toDebugString}")

    // 保存模型
    // model.save(sc, "target/tmp/myRandomForestClassificationModel")
    // val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

    sc.stop()
  }

  def loadFile(
                sc: SparkContext,
                path: String,
                numFeatures: Int,
                minPartitions: Int): RDD[LabeledPoint] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !line.isEmpty)
      .map { line =>
        val items = line.split(',')
        val label = items.head.toDouble

        val indices = (1 to 1138).toArray
        val values = items.tail.filter(_.nonEmpty).map(item => item.toDouble)

        (label, indices, values)

      }

    parsed.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
    }
  }

}
