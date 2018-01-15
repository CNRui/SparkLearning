package SparkML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * 基于DataFrame的LogisticRegression，用Pipeline实现
  */
object LogisticRegressionPipeline {

  def loadFile3(sc: SparkContext,
                path: String,
                minPartitions: Int): RDD[(Double, Vector)] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !line.isEmpty)
      .map { line =>
        val items = line.split(',')
        //var label = ""
        val label = items.head.toDouble
        //label=if (label0==1) "A" else "B"
        val values = items.tail.filter(_.nonEmpty).map(item => item.toDouble)

        (label, values)
      }
    parsed.map { case (label, values) =>
      (label, Vectors.dense(values))
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("LogisticRegressionPipeline").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    val raw = loadFile3(sc, "E:\\文档\\lc_bubble case\\glass_test\\lc_bubble_spark.txt", 2)
    val data = sqlC.createDataFrame(raw).toDF("label", "features")

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setMaxIter(100)
     // .setRegParam(0.02)
      .setThreshold(0.55)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, lr))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    predictions.where("label != prediction").show(5)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val precision = evaluator.evaluate(predictions)
    println(s"precision  = $precision")

    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("recall")
    val recall = evaluator1.evaluate(predictions)
    println(s"recall  = $recall")

    val predict = predictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = predictions.select("label").rdd.map(_.getDouble(0))

    val predAndLabels = predict.zip(labels)
    val metrics = new BinaryClassificationMetrics(predAndLabels)
    val auROC = metrics.areaUnderROC()
    val auPRC = metrics.areaUnderPR()

    println("Area under ROC = " + auROC)
    println("Area under PRC = " + auPRC)

    val metrics1 = new MulticlassMetrics(predAndLabels)
    println("Confusion matrix:")
    println(metrics1.confusionMatrix)

    sc.stop()
  }

}
