package SparkML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 基于DataFrame的RandomForestClassification
  */
object RandomForestTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("RandomForestTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    val raw = loadFile2(sc, "E:\\文档\\lc_bubble case\\glass_test\\lc_bubble_spark.txt", 2)
    val data = sqlC.createDataFrame(raw).toDF("label", "features")

    // 打标签，会按照样本数量从多到少进行，indexedLabel：0 ->多数样本  1 ->少数样本
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    // 特征向量化，由于读取文件时已经完成向量化，这里不用再做
    /*  val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(data)*/

    // 拆分训练集测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 迅雷随机森林模型
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(10)

    // 将预测标签转换回去得到原始Label
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // 构建Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, rf, labelConverter))

    // 训练模型
    val model = pipeline.fit(trainingData)

    // 模型预测
    val predictions = model.transform(testData)

    // 预测结果展示及结果评估
    predictions.select("predictedLabel", "label", "features").show(5)
    predictions.where("label != predictedLabel").show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val precision = evaluator.evaluate(predictions)
    println(s"precision  = $precision")

    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("recall")
    val recall = evaluator1.evaluate(predictions)
    println(s"recall  = $recall")

    //AUC.PRC.混淆矩阵
    val predict = predictions.select("predictedLabel").rdd.map(_.toString.substring(1, 2).toDouble)
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

    //Spark1.6 要求rawPredictionCol为VectorUDT类型，普通DataFrame会报错
    /*    val evaluator3 = new BinaryClassificationEvaluator()
          .setLabelCol("indexedLabel")
          .setRawPredictionCol("prediction")
          .setMetricName("areaUnderROC")
        val auc = evaluator3.evaluate(predictions)
        println(s"auc  = $auc")*/

    //    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    //    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
    sc.stop()
  }

  def loadFile2(sc: SparkContext,
                path: String,
                minPartitions: Int): RDD[(Double, Vector)] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !line.isEmpty)
      .map { line =>
        val items = line.split(',')
        // var label = ""
        val label = items.head.toDouble
        // label = if (label0 == 1) "A" else "B"
        val values = items.tail.filter(_.nonEmpty).map(item => item.toDouble)
        (label, values)
      }
    parsed.map { case (label, values) =>
      (label, Vectors.dense(values))
    }
  }
}