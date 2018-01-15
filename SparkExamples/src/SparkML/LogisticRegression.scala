package SparkML

import SparkML.RandomForestTest.loadFile2
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.{Row, SQLContext}


/**
  * 基于DataFrame的LogisticRegression
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("LogisticRegression").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    val raw = loadFile2(sc, "E:\\文档\\lc_bubble case\\glass_test\\lc_bubble_spark.txt", 2)

    val data = sqlC.createDataFrame(raw).toDF("label", "features")

    // 拆分训练集测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 创建逻辑回归模型
    val lr = new LogisticRegression()

    lr.setMaxIter(10)
      .setRegParam(0.01)

    // 训练模型
    val model1 = lr.fit(trainingData)

    // 另一种设置模型参数的方法
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 40) // maxIter从20改到40
      .put(lr.regParam -> 0.05, lr.threshold -> 0.50)

    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ paramMap2

    // 用设置的参数训练模型
    val model2 = lr.fit(trainingData, paramMapCombined)

    val trainingSummary = model2.summary
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // 未经过测试之前的模型AUC，训练模型的AUC
    val roc = binarySummary.roc
    roc.show()
    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

    // 模型预测
    val predict = model2.transform(testData)

    predict.select("label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(label: Double, prob, prediction: Double) =>
        println(s"($label) -> prob=$prob, prediction=$prediction")
      }

    //AUC.PRC.混淆矩阵
    val predictions = predict.select("prediction").rdd.map(_.getDouble(0))
    val labels = predict.select("label").rdd.map(_.getDouble(0))

    val predAndLabels = predictions.zip(labels)
    val metrics = new BinaryClassificationMetrics(predAndLabels)
    val auROC = metrics.areaUnderROC()
    val auPRC = metrics.areaUnderPR()

    println("Area under ROC = " + auROC)
    println("Area under PRC = " + auPRC)

    val metrics1 = new MulticlassMetrics(predAndLabels)
    println("Confusion matrix:")
    println(metrics1.confusionMatrix)
  }

}
