package util

import java.io.{File, FileOutputStream, PrintWriter}

import main.AuthorNetwork.{EdgeML, VertexAttr}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, Word2Vec}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

object Training {
  /**
    * 从图的边rdd中构造包含作者名字的训练数据
    *
    * @param ss SparkSession
    * @param graph 作者网络图
    * @return
    */
  def getDataIncludingName(ss: SparkSession, graph: Graph[VertexAttr, EdgeML]): DataFrame = {
    val row = graph.triplets.filter(_.attr._1 != 2).map(x =>
      //小于0的余弦相似分数置为0;;过滤小于0的相似分数.
      (x.attr._1, new SparseVector(size = 4, indices = Array(0, 1, 2, 3),
        values = Array(Math.max(0, x.attr._2), Math.max(0, x.attr._3), Math.max(0, x.attr._4), Math.max(0, x.attr._5))).toSparse, x.srcAttr._1)
    )
    import ss.implicits._
    val df = row.toDF("label", "features", "name")
    //df.show(20, truncate = false)
    df
  }

  /**
    * 从图的边rdd中构造不包含作者名字的训练数据
    *
    * @param ss
    * @param graph
    * @return
    */
  def getData(ss: SparkSession, graph: Graph[VertexAttr, EdgeML]): DataFrame = {
    val row = graph.edges.filter(_.attr._1 != 2).map(x =>
      //小于0的余弦相似分数置为0;;过滤小于0的相似分数.
      (x.attr._1, new SparseVector(size = 4, indices = Array(0, 1, 2, 3),
        values = Array(Math.max(0, x.attr._2), Math.max(0, x.attr._3), Math.max(0, x.attr._4), Math.max(0, x.attr._5))).toSparse)
    )
    import ss.implicits._
    val df = row.toDF("label", "features")
    //df.show(20, truncate = false)
    df
  }

  /**
    * 训练tfidf模型，将文献数据转换为向量表示
    *
    * @param srcDF
    * @param inputCol
    * @param outputCol
    * @return
    */
  def fitTfIdf(srcDF: DataFrame, inputCol: String, outputCol: String): DataFrame = {
    //分词器
    val tokenizer = new Tokenizer().setInputCol(inputCol).setOutputCol("words")
    //分词结果存储到dataframe
    val wordsData = tokenizer.transform(srcDF)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures")
    //.setNumFeatures(1000)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol(outputCol)
    val idfModel = idf.fit(featurizedData)

    var rescaledData = idfModel.transform(featurizedData)
    rescaledData.drop("rawFeatures")
  }

  def saveLibsvm(ss: SparkSession, graph: Graph[VertexAttr, EdgeML], path: String) {
    def transform: Edge[EdgeML] => String = x => {
      val label = x.attr._1
      val orgSim = x.attr._2
      val coauthorSim = x.attr._3
      val titleSim = x.attr._4
      val abstractSim = x.attr._5

      val line = s"$label 1:$orgSim 2:$coauthorSim 3:$titleSim 4:$abstractSim \n"
      line
    }

    val file = new File(path + "/libsvm.txt")
    if (!file.exists()) {
      file.createNewFile()
    }
    graph.edges.foreach(x => {
      if (x.attr._1 != 2) {
        val line = transform(x)
        //        println(line)
        val pw = new PrintWriter(new FileOutputStream(
          file, true))
        pw.append(line)
        pw.flush()
        pw.close()
      }
    })
  }

  /**
    * 训练word2vec向量模型
    *
    * @param srcDF     源dataframe
    * @param inputCol  输入列名
    * @param outputCol 输出列名
    * @return
    */
  def fitWord2Vec(srcDF: DataFrame, inputCol: String, outputCol: String): DataFrame = {
    //分词器
    val tokenizer = new Tokenizer().setInputCol(inputCol).setOutputCol("words")
    //分词结果存储到dataframe
    val wordsData = tokenizer.transform(srcDF.na.fill("", Array(inputCol))).drop(inputCol)
    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol(outputCol)
      .setVectorSize(100)
      .setMinCount(0)
    val model = word2Vec.fit(wordsData)
    val result = model.transform(wordsData)
    result.drop("words")
  }

  /**
    * 训练多层感知器分类器
    */
  def trainByMPC(ss: SparkSession, data: DataFrame): MultilayerPerceptronClassificationModel = {

    val splits = data.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    println(s"data:${data.count()}")


    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](4, 5, 4, 2)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    // train the model
    val model = trainer.fit(train)

    // compute accuracy on the test set
    val result = model.transform(test)
    println(s"result:${result.count()}")
    val predictionAndLabels = result.select("prediction", "label")
    //    predictionAndLabels.filter("label==prediction and label==1.0").show(1000)
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    predictionAndLabels.createTempView("pred")
    val fp = ss.sql("select * from pred where label!=prediction and label==0.0").count().toDouble

    val tp = ss.sql("select * from pred where label==prediction and label==1.0").count().toDouble

    val fn = ss.sql("select * from pred where label!=prediction and label==1.0").count().toDouble

    val precision = tp / (tp + fp)
    val recall = tp / (tp + fn)
    val fscore = 2 * recall * precision / (recall + precision)
    println(s"tp=$tp,fp=$fp,fn=$fn")
    println(s"precision=$precision")
    println(s"recall=$recall")
    println(s"fscore=$fscore")
    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
    ss.catalog.dropTempView("pred")
    model
  }

  def trainByLSVC(ss: SparkSession, data: DataFrame): LinearSVCModel = {

    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)
    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)

    // Fit the model
    val lsvcModel = lsvc.fit(training)

    // Print the coefficients and intercept for linear svc
    println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")
    lsvcModel
  }

  /**
    * 训练逻辑回归分类器
    *
    * @param ss   sparksession
    * @param data 用于训练的原始数据
    */
  def trainByLR(ss: SparkSession, data: DataFrame): LogisticRegressionModel = {
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)

    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setFamily("binomial")
    //1: l2 ridge regression 0: l1 lasso regression
    //.setElasticNetParam(1)mkdir data

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()

      .addGrid(lr.regParam, (0.01 to 0.1 by 0.01).toArray)
      .addGrid(lr.fitIntercept)
      //.addGrid(lr.threshold,(0.3 to 0.8 by 0.1).toArray)
      .addGrid(lr.elasticNetParam, (0.0 to 1.0 by 0.1).toArray)
      .build()

    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)
      // Evaluate up to 2 parameter settings in parallel
      .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)

    //     Make predictions on test data. model is the model with combination of parameters
    //     that performed best.
    val result = model.transform(test)
      .filter("label==1.0")
      .select("features", "label", "prediction")
    //result.show(numRows = 1000, truncate = false)

    val lrModel = model.bestModel.asInstanceOf[LogisticRegressionModel]


    //从训练好的逻辑回归模型中提取摘要
    import ss.implicits._

    val trainingSummary = lrModel.binarySummary

    // 获得每次迭代的目标函数返回值，打印每次迭代的损失
    //    val objectiveHistory = trainingSummary.objectiveHistory
    //    println("objectiveHistory:")
    //    objectiveHistory.foreach(loss => println(loss))

    // 求ROC曲线，打印areaUnderROC（ROC曲线下方的面积）
    //    val roc = trainingSummary.roc
    //    roc.show()
    //    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    //  设置模型阈值以最大化F-Measure
    val fMeasure = trainingSummary.fMeasureByThreshold
    //fMeasure.show(10)
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)
    //
    //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //println(s"bestThreshold: $bestThreshold maxFMeasure: $maxFMeasure")
    lrModel.setThreshold(bestThreshold)
    //    lrModel

    result.createTempView("pred")
    val fp = ss.sql("select * from pred where label!=prediction and label==0.0").count().toDouble

    val tp = ss.sql("select * from pred where label==prediction and label==1.0").count().toDouble

    val fn = ss.sql("select * from pred where label!=prediction and label==1.0").count().toDouble

    val precision = tp / (tp + fp)
    val recall = tp / (tp + fn)
    val fscore = 2 * recall * precision / (recall + precision)
    println(s"tp=$tp,fp=$fp,fn=$fn")
    println(s"precision=$precision")
    println(s"recall=$recall")
    println(s"fscore=$fscore")
    ss.catalog.dropTempView("pred")
    lrModel
  }

  def trainByNB(ss: SparkSession, data: DataFrame): NaiveBayesModel = {
    // Load and parse the data file.
    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2))
    val model = new NaiveBayes()
      .fit(training)

    val predictions = model.transform(test)
    predictions.show()

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")

    model
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder
      .appName("train")
      .master("local[*]") //设置运行环境为本地模式
      .getOrCreate()


    ss.stop()
  }
}
