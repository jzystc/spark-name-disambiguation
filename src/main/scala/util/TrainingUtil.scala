package util

import java.io.{File, FileOutputStream, PrintWriter}

import com.alibaba.fastjson.JSONObject
import na.AuthorNetwork.{EdgeAttr, VertexAttr}
import na.AuthorNetwork
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier, _}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, ClusteringEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, StdIn}
import scala.util.control.Breaks._

object TrainingUtil {
  def generateMissingFeaturePredictionTrainingData(ss: SparkSession, data: DataFrame): Unit = {

  }

  def trainRegression(ss: SparkSession, training: DataFrame): GeneralizedLinearRegressionModel = {

    // Load training data
    //    val training = ss.read.format("libsvm")
    //      .load("data/mllib/sample_linear_regression_data.txt")
    //    val lr = new LinearRegression()
    //      .setMaxIter(10)
    //      .setRegParam(0.3)
    //      .setElasticNetParam(0.8)
    //    // Fit the model
    //    val lrModel = lr.fit(training)
    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)
    val model = glr.fit(training)
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val summary = model.summary
    println(s"numIterations: ${summary.numIterations}")
    //    summary.numIterations
    //    println(s"numIterations: ${summary.totalIterations}")
    //    println(s"objectiveHistory: [${summary.objectiveHistory.mkString(",")}]")
    summary.residuals.show()
    //    println(s"RMSE: ${summary.rootMeanSquaredError}")
    //    println(s"r2: ${summary.r2}")
    model
  }


  /**
   * 读取papers表中每篇文章的标题和摘要信息,并进行连接
   *
   * @param ss
   * @return dataframe
   */
  def getRawTextFromDB(ss: SparkSession): DataFrame = {
    val dfReader = DBUtil.getDataFrameReader(ss)
    val authorsDF = dfReader.option("dbtable", "authors").load()
    val papersDF = dfReader.option("dbtable", "papers") load()
    authorsDF.createTempView("authors")
    papersDF.createTempView("papers")
    val org = ss.sql("select org from authors").toDF("text")
    import ss.sqlContext.implicits._
    val titleAbstract = ss.sql("select title,abstract from papers")
    //迭代"连接"/"合并"文本,","隔开???空格是不是更合适" ".
    val mergedTitleAbstract = titleAbstract.map(_.toSeq.foldLeft("")(_ + "," + _).substring(1)).toDF("text")
    //union完成DataFrame合并
    val text = org.union(mergedTitleAbstract).filter("text!=\"\"")
    //RegexTokenizer基于正则表达式.setPattern(re),设置分词
    //val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    //分词结果存储到dataframe
    text
  }

  /**
   * 训练word2vec模型
   *
   * @param ss            sparkSession
   * @param textDF        原始文本dataframe
   * @param vectorSize    向量维度
   * @param minCount      词语的最小出现次数
   * @param modelSavePath 模型保存路径
   */
  def trainWord2Vec(ss: SparkSession, textDF: DataFrame, vectorSize: Int = 100, minCount: Int = 3,
                    windowSize: Int = 3, modelSavePath: String): Unit = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern("\\W")
      .setToLowercase(true)

    val words = regexTokenizer.transform(textDF).drop("text")

    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("features")
      .setVectorSize(vectorSize)
      .setMinCount(minCount)
      .setWindowSize(windowSize)

    val model = word2Vec.fit(words)
    val modelName = s"word2vec_$vectorSize"
    model.write.overwrite().save(modelSavePath + "/" + modelName)
  }

  /**
   * 读取模型
   *
   * @param path 模型保存位置
   * @return
   */
  def loadWord2VecModel(path: String): Word2VecModel = {
    val model = Word2VecModel.load(path)
    model
  }

  /**
   * 将文本转换为向量
   *
   * @param data      原始dataframe
   * @param inputCol  输入列名
   * @param outputCol 输出列名
   * @param model     word2vec模型
   * @return
   */
  def transSentence2Vec(data: DataFrame, inputCol: String, outputCol: String, model: Word2VecModel, removeStopWords: Boolean = false): DataFrame = {
    //val tokenizer = new Tokenizer().setInputCol(inputCol).setOutputCol("words")
    //分词结果存储到dataframe
    if (removeStopWords) {
      val regexTokenizer = new RegexTokenizer().setInputCol(inputCol).setOutputCol("tokenized words")
      val tokenizedWords = regexTokenizer.transform(data.na.fill("", Array(inputCol))).drop(inputCol)
      val stopWordsRemover = new StopWordsRemover().setInputCol("tokenized words").setOutputCol("words")
      val noStopWords = stopWordsRemover.transform(tokenizedWords).drop("tokenized words")
      model.setInputCol("words").setOutputCol(outputCol)
      val result = model.transform(noStopWords)
      result.drop("words")
    } else {
      val regexTokenizer = new RegexTokenizer().setInputCol(inputCol).setOutputCol("words")
      val tokenizedWords = regexTokenizer.transform(data.na.fill("", Array(inputCol))).drop(inputCol)
      model.setInputCol("words").setOutputCol(outputCol)
      val result = model.transform(tokenizedWords)
      result.drop("words")
    }

  }

  def getCorpus(ss: SparkSession): DataFrame = {
    val path = "d:/namedis/"
    //val text=getRowText(ss)
    //text.write.parquet(path+"rowtext")
    import ss.sqlContext.implicits._
    val aff = ss.sparkContext.textFile("D:\\Users\\JZY\\PycharmProjects\\dataprocessing\\src\\aff.txt").toDF()
    val text = ss.sparkContext.textFile("D:\\Users\\JZY\\PycharmProjects\\dataprocessing\\src\\text.txt").toDF()
    val rawText = ss.read.parquet(path + "rawtext").union(aff).union(text)
    rawText.show(10)
    //    rawText.write.parquet("d:/namedis/corpus/")
    rawText
  }

  /**
   * 从图的边rdd中构造包含作者名字的训练数据
   *
   * @param ss    SparkSession
   * @param graph 作者网络图
   * @return
   */
  def getDataIncludingName(ss: SparkSession, graph: Graph[VertexAttr, EdgeAttr]): DataFrame = {
    val row = graph.triplets.filter(_.attr._1 != 2).map(x => {
      //小于0的余弦相似分数置为0;;过滤小于0的相似分数.
      val indices = Array(0, 1, 2, 3)
      (x.attr._1, new SparseVector(size = indices.length, indices = indices,
        values = Array(Math.max(0, x.attr._2), Math.max(0, x.attr._3), Math.max(0, x.attr._4))).toSparse, x.srcAttr._1)
    })
    import ss.implicits._
    val df = row.toDF("label", "features", "name")
    //df.show(20, truncate = false)
    df
  }


  /**
   * 从图的边rdd中构造不包含作者名字的训练数据
   *
   * @param ss
   * @param edgeRDD
   * @return
   */
  def getData(ss: SparkSession, edgeRDD: EdgeRDD[EdgeAttr]): DataFrame = {
    val row = edgeRDD.filter(_.attr._1 != 2).map(x =>
      //小于0的余弦相似分数置为0;;过滤小于0的相似分数.
    {
      val indices = Array(0, 1, 2, 3, 4)
      (x.attr._1, new SparseVector(size = indices.length, indices = indices,
        values = Array(Math.max(0, x.attr._2), x.attr._3, Math.max(0, x.attr._4), x.attr._5, Math.max(0, x.attr._6))).toSparse)
      //      values = Array(0, Math.max(0, x.attr._3), Math.max(0, x.attr._4), Math.max(0, x.attr._5))).toSparse)
      //      values = Array(Math.max(0, x.attr._2),0, Math.max(0, x.attr._4), Math.max(0, x.attr._5))).toSparse)
      //      values = Array(Math.max(0, x.attr._2), Math.max(0, x.attr._3), 0, Math.max(0, x.attr._5))).toSparse)
      //      values = Array(Math.max(0, x.attr._2), Math.max(0, x.attr._3), Math.max(0, x.attr._4), 0)).toSparse)
    }
    )
    import ss.implicits._
    val df = row.toDF("label", "features")
    df.show(10, truncate = 0)
    df
    //    val scaler = new MinMaxScaler()
    //      .setInputCol("features")
    //      .setOutputCol("scaledFeatures")
    //    val scalerModel = scaler.fit(df)
    //    val scaledData = scalerModel.transform(df).drop("features").withColumnRenamed("scaledFeatures", "features")
    //    //df.show(20, truncate = false)
    //    scaledData
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

  /**
   * 将训练数据保存为libsvm格式
   *
   * @param ss   sparkSession
   * @param data 训练数据
   * @param path 保存路径
   */
  def dumpRDD2Libsvm(ss: SparkSession, data: EdgeRDD[EdgeAttr], path: String, fname: String = "libsvm.txt") {
    def transform: Edge[EdgeAttr] => String = x => {
      val label = x.attr._1
      val orgSim = x.attr._2
      val coauthorSim = x.attr._3
      val textSim = x.attr._4
      val yearSim = x.attr._5
      val venueSim = x.attr._6
      // val titleSim = x.attr._4
      // val abstractSim = x.attr._5
      // val line = s"$label 1:$orgSim 2:$coauthorSim 3:$titleSim 4:$abstractSim \n"
      val line = s"$label 1:$orgSim 2:$coauthorSim 3:$textSim 4:$yearSim 5:$venueSim\n"
      line
    }

    val file = new File(s"$path/$fname")
    if (!file.exists()) {
      file.createNewFile()
    }
    val libsvm = data.map(x => transform(x)).collect()
    val pw = new PrintWriter(new FileOutputStream(
      file, true))
    libsvm.foreach(x => {
      pw.append(x)
    })
    pw.flush()
    pw.close()
  }

  def dumpDataForFeaturePrediction2Libsvm(ss: SparkSession, data: DataFrame, path: String, fname: String) {
    import ss.implicits._
    def transform: Row => String = x => {
      val label = x(0).asInstanceOf[Double]
      val features = x(1).asInstanceOf[SparseVector]
      // val titleSim = x.attr._4
      // val abstractSim = x.attr._5
      // val line = s"$label 1:$orgSim 2:$coauthorSim 3:$titleSim 4:$abstractSim \n"
      //      if (features.head != 0) {
      //        val line = s"$label 1:${features.head} 2:${features(1)} 3:${features(2)} 4:${features(3)} 5:${features(4)}\n"
      //        line
      //      } else {
      //        ""
      //      }
      val line = s"$label 1:${features.values.head} 2:${features.values(1)} 3:${features.values(2)} 4:${features.values(3)}\n"
      line
    }

    val file = new File(s"$path/$fname")
    if (!file.exists()) {
      file.createNewFile()
    }
    val libsvm = data.map(x => transform(x)).collect()
    val pw = new PrintWriter(new FileOutputStream(
      file, true))
    libsvm.foreach(x => {
      pw.append(x)
      //      if (!x.equals("")) {
      //        pw.append(x)
      //      }
    })
    pw.flush()
    pw.close()
  }

  def dumpDF2Libsvm(ss: SparkSession, data: DataFrame, path: String, fname: String = "libsvm.txt") {
    import ss.implicits._
    def transform: Row => String = x => {
      val label = x(0).asInstanceOf[Double]
      val features = x(1).asInstanceOf[Seq[Double]]
      // val titleSim = x.attr._4
      // val abstractSim = x.attr._5
      // val line = s"$label 1:$orgSim 2:$coauthorSim 3:$titleSim 4:$abstractSim \n"
      //      if (features.head != 0) {
      //        val line = s"$label 1:${features.head} 2:${features(1)} 3:${features(2)} 4:${features(3)} 5:${features(4)}\n"
      //        line
      //      } else {
      //        ""
      //      }
      val line = s"$label 1:${features.head} 2:${features(1)} 3:${features(2)} 4:${features(3)} 5:${features(4)}\n"
      line
    }

    val file = new File(s"$path/$fname")
    if (!file.exists()) {
      file.createNewFile()
    }
    val libsvm = data.map(x => transform(x)).collect()
    val pw = new PrintWriter(new FileOutputStream(
      file, true))
    libsvm.foreach(x => {
      pw.append(x)
      //      if (!x.equals("")) {
      //        pw.append(x)
      //      }
    })
    pw.flush()
    pw.close()
  }

  /**
   * 训练word2vec向量模型
   *
   * @param corpus    源dataframe
   * @param inputCol  输入列名
   * @param outputCol 输出列名
   * @return
   */
  def fitWord2Vec(corpus: DataFrame, inputCol: String, outputCol: String, minCount: Int = 5, vecSize: Int = 100, rmStopWord: Boolean = false): DataFrame = {
    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol(outputCol)
      .setVectorSize(vecSize)
      .setMinCount(minCount)
    //分词器
    //    val tokenizer = new Tokenizer().setInputCol(inputCol).setOutputCol("words")
    val regexTokenizer = new RegexTokenizer().setInputCol(inputCol).setOutputCol("tokenized words")
    //分词结果存储到dataframe
    val tokenizedWords = regexTokenizer.transform(corpus.na.fill("", Array(inputCol))).drop(inputCol)

    if (rmStopWord) {
      val stopWordsRemover = new StopWordsRemover().setInputCol("tokenized words").setOutputCol("words")
      val noStopWords = stopWordsRemover.transform(tokenizedWords).drop("tokenized words")
      val model = word2Vec.fit(noStopWords)
      val result = model.transform(noStopWords)
      result.drop("words")
    } else {
      val model = word2Vec.setInputCol("tokenized words").fit(tokenizedWords)
      val result = model.transform(tokenizedWords)
      result.drop("tokenized words")
    }
  }

  def computePRF(ss: SparkSession, df: DataFrame): (Double, Double, Double) = {
    df.createTempView("df")
    val fp = ss.sql("select * from df where label!=prediction and label==0.0").count().toDouble

    val tp = ss.sql("select * from df where label==prediction and label==1.0").count().toDouble

    val fn = ss.sql("select * from df where label!=prediction and label==1.0").count().toDouble
    val precision = tp / (tp + fp)
    val recall = tp / (tp + fn)
    val fscore = 2 * recall * precision / (recall + precision)
    println(s"tp=$tp,fp=$fp,fn=$fn")
    println(s"precision=$precision")
    println(s"recall=$recall")
    println(s"f-score=$fscore")
    (precision, recall, fscore)
  }

  def trainRandomForest(ss: SparkSession, data: DataFrame): RandomForestClassificationModel = {
    // Load and parse the data file, converting it to a DataFrame.
    //  val data = ss.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    //    val labelIndexer = new StringIndexer()
    //      .setInputCol("label")
    //      .setOutputCol("indexedLabel")
    //      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    //    val featureIndexer = new VectorIndexer()
    //      .setInputCol("features")
    //      .setOutputCol("indexedFeatures")
    //      .setMaxCategories(4)
    //      .fit(data)
    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))
    // Train a RandomForest model.
    //    val rf = new RandomForestClassifier()
    //      .setLabelCol("indexedLabel")
    //      .setFeaturesCol("indexedFeatures")
    //      .setNumTrees(10)
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
    //          .setInputCol("prediction")
    //          .setOutputCol("predictedLabel")
    // Convert indexed labels back to original labels.
    //    val labelConverter = new IndexToString()
    //      .setInputCol("prediction")
    //      .setOutputCol("predictedLabel")
    //      .setLabels(labelIndexer.labels)
    // Chain indexers and forest in a Pipeline.
    //    val pipeline = new Pipeline()
    //      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
    // Train model. This also runs the indexers.
    val model = rf.fit(trainingData)
    // Make predictions.
    val predictions = model.transform(testData)
    //    // Select example rows to display.
    //    //    predictions.select("predictedLabel", "label", "features").show(5)
    //    predictions.select("prediction", "label").show(5)
    //    // Select (prediction, true label) and compute test error.
    val evaluator = new BinaryClassificationEvaluator()
    //      .setLabelCol("label")
    //                  .setPredictionCol("prediction")
    //      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")
    val rfModel = model.asInstanceOf[RandomForestClassificationModel]
    //  println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
    rfModel
  }

  /**
   * 训练多层感知器分类器
   */
  def trainByMPC(ss: SparkSession, data: DataFrame, maxIter: Int = 10): MultilayerPerceptronClassificationModel = {

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
      .setMaxIter(maxIter)

    // train the model
    val model = trainer.fit(train)

    // compute accuracy on the test set
    val result = model.transform(test)
    println(s"result:${result.count()}")
    val predictionAndLabels = result.select("prediction", "label")
    //    predictionAndLabels.filter("label==prediction and label==1.0").show(1000)
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    computePRF(ss, predictionAndLabels)
    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
    ss.catalog.dropTempView("pred")
    model
  }

  /**
   * 训练线性支持向量机模型
   *
   * @param ss
   * @param data    训练数据 dataframe
   * @param maxIter 最大迭代次数
   * @return
   */
  def trainSVM(ss: SparkSession, data: DataFrame, maxIter: Int = 10): LinearSVCModel = {
    //    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)
    // 定义超参数集合
    val lsvc = new LinearSVC()
    val paramGrid = new ParamGridBuilder()
      //      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lsvc.regParam, Array(0.1, 0.01))
      .build()
    //    val pipeline = new Pipeline().setStages(Array(lsvc))
    // 定义验证器
    val cv = new CrossValidator()
      .setEstimator(lsvc)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3) // Use 3+ in practice
    val cvModel = cv.fit(data)
    val lsvcModel = cvModel.bestModel.asInstanceOf[LinearSVCModel]
    //      .setMaxIter(maxIter)
    //      .setRegParam(0.1)
    // Fit the model
    //    val lsvcModel = lsvc.fit(data)
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
  def trainLR(ss: SparkSession, data: DataFrame, maxIter: Int = 10): LogisticRegressionModel = {
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LogisticRegression()
      .setMaxIter(maxIter)
      .setFamily("binomial")

    //1: l2 ridge regression 0: l1 lasso regression
    //.setElasticNetParam(1)mkdir data

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()

      .addGrid(lr.regParam, (0.01 to 0.1 by 0.01).toArray)
      //.addGrid(lr.fitIntercept)
      //.addGrid(lr.threshold,(0.4 to 0.7 by 0.1).toArray)
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
    //    val result = model.transform(test)
    //      .filter("label==1.0")
    //      .select("features", "label", "prediction")
    //result.show(numRows = 1000, truncate = false)

    val lrModel = model.bestModel.asInstanceOf[LogisticRegressionModel]


    //从训练好的逻辑回归模型中提取摘要

    //    val trainingSummary = lrModel.binarySummary

    // 获得每次迭代的目标函数返回值，打印每次迭代的损失
    //    val objectiveHistory = trainingSummary.objectiveHistory
    //    println("objectiveHistory:")
    //    objectiveHistory.foreach(loss => println(loss))

    // 求ROC曲线，打印areaUnderROC（ROC曲线下方的面积）
    //    val roc = trainingSummary.roc
    //    roc.show()
    //    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    //  设置模型阈值以最大化F-Measure
    //    val fMeasure = trainingSummary.fMeasureByThreshold
    //fMeasure.show(10)
    //    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    //    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
    //      .select("threshold").head().getDouble(0)
    //
    //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //println(s"bestThreshold: $bestThreshold maxFMeasure: $maxFMeasure")
    //lrModel.setThreshold(bestThreshold)
    //    lrModel
    //
    //    result.createTempView("pred")
    //    val fp = ss.sql("select * from pred where label!=prediction and label==0.0").count().toDouble
    //
    //    val tp = ss.sql("select * from pred where label==prediction and label==1.0").count().toDouble
    //
    //    val fn = ss.sql("select * from pred where label!=prediction and label==1.0").count().toDouble
    //
    //    val precision = tp / (tp + fp)
    //    val recall = tp / (tp + fn)
    //    val fscore = 2 * recall * precision / (recall + precision)
    //    println(s"tp=$tp,fp=$fp,fn=$fn")
    //    println(s"precision=$precision")
    //    println(s"recall=$recall")
    //    println(s"fscore=$fscore")
    //    ss.catalog.dropTempView("pred")
    println("coefficients", lrModel.coefficients)
    println("intercept", lrModel.intercept)
    println("threshold", lrModel.getThreshold)
    println("ElasticNetParam", lrModel.getElasticNetParam)
    println("regParam", lrModel.getRegParam)
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

  def loadCorpus(ss: SparkSession, textTxtPath: String, orgTxtPath: String = "", venueTxtPath: String = ""): DataFrame = {
    import ss.implicits._
    var text = ss.sparkContext.textFile(textTxtPath).toDF("text")
    if (orgTxtPath != "") {
      val organizations = ss.sparkContext.textFile(orgTxtPath).toDF("text")
      text = text.union(organizations)
    }
    if (venueTxtPath != "") {
      val venues = ss.sparkContext.textFile(venueTxtPath).toDF("text")
      text = text.union(venues)
    }
    text
  }

  def testWord2vec(ss: SparkSession): Unit = {
    val model = Word2VecModel.load("d:/namedis/word2vec_100")
    val df = ss.createDataFrame(Seq(
      (Array("dense"), 1), (Array("sparse"), 2)
    )).toDF("words", "id")
    val vector = model.transform(df)
    val rdd = vector.rdd
    val a = rdd.map(x => x(2)).collect()
    val sim = SimilarityUtil.cosine(a(0).asInstanceOf[Vector], a(1).asInstanceOf[Vector])
    println(sim)
  }

  def loadLRModel(modelPath: String): Unit = {
    val lr = LogisticRegressionModel.load(modelPath)
    println(s"coefficients:${lr.coefficients}")
    println(s"intercept:${lr.intercept}")
    println(s"threshold:${lr.getThreshold}")
    println(s"elastic net:${lr.getElasticNetParam}")
    println(s"reg:${lr.getRegParam}")
  }

  /**
   * 读取libsvm格式的训练数据并进行聚类
   *
   * @param ss
   * @param dataPath libsvm格式文件的位置
   */
  def clusterData(ss: SparkSession, dataPath: String, numPartitions: Int): DataFrame = {
    // Trains a k-means model.
    val data = ss.read.format("libsvm").option("numFeatures", "5").load(dataPath).repartition(numPartitions)

    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(data)
    // Make predictions
    val predictions = model.transform(data)
    // Evaluate clustering by computing Silhouette score
    //    val evaluator = new ClusteringEvaluator()
    //    val silhouette = evaluator.evaluate(predictions)
    //    println(s"Silhouette with squared euclidean distance = $silhouette")
    // Shows the result.
    //    println("Cluster Centers: ")
    //    model.clusterCenters.foreach(println)
    //    predictions.write.save(resultSavePath)
    predictions
    //    predictions.createTempView("kmeans")
    //    val cnt0 = ss.sqlContext.sql("select * from kmeans where prediction==0")
    //    val cnt1 = ss.sqlContext.sql("select * from kmeans where prediction==1")
    //    cnt0.show(10, truncate = 0)
    //    cnt1.show(10, truncate = 0)
  }

  /**
   * 读取libsvm格式的训练数据并进行聚类
   *
   * @param ss
   */
  def groupData(ss: SparkSession, dataset: DataFrame, numPartitions: Int): DataFrame = {
    // Trains a k-means model.
    println("[cluster]")
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)
    // Make predictions
    val predictions = model.transform(dataset)
    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    //    println(s"Silhouette with squared euclidean distance = $silhouette")
    // Shows the result.
    //    println("Cluster Centers: ")
    //    model.clusterCenters.foreach(println)
    //    predictions.createTempView("kmeans")
    //    val cnt0 = ss.sqlContext.sql("select * from kmeans where prediction==0")
    //    val cnt1 = ss.sqlContext.sql("select * from kmeans where prediction==1")
    //    cnt0.show(10, truncate = 0)
    //    cnt1.show(10, truncate = 0)
    //    ss.sqlContext.dropTempTable("kmeans")
    predictions
  }

  def balance(ss: SparkSession, df: DataFrame): DataFrame = {
    ss.sqlContext.dropTempTable("train")
    df.createTempView("train")
    val label0 = ss.sqlContext.sql("select * from train where label==0")
    val label1 = ss.sqlContext.sql("select * from train where label==1")
    val num0 = label0.count().toDouble
    if (num0 == 0) {
      label1
    } else {
      val num1 = label1.count().toDouble

      if (num0 > num1) {
        val rate = num1 / num0
        val negative = label0.sample(withReplacement = false, rate)
        label1.union(negative)
      } else {
        val rate = num0 / num1
        val positive = label1.sample(withReplacement = false, rate)
        positive.union(label0)
      }
    }
  }

  def sampling(ss: SparkSession, df: DataFrame, sampleWay: Int): DataFrame = {
    df.createTempView("kmeans")
    val c0 = ss.sqlContext.sql("select * from kmeans where prediction==0")
    c0.show(5, truncate = 0)
    val c01 = ss.sqlContext.sql("select * from kmeans where prediction==0 and label==1")
    val c00 = ss.sqlContext.sql("select * from kmeans where prediction==0 and label==0")
    //    cnt0_1.show(50, truncate = 0)
    val c1 = ss.sqlContext.sql("select * from kmeans where prediction==1")
    c1.show(5, truncate = 0)
    val c11 = ss.sqlContext.sql("select * from kmeans where prediction==1 and label==1")
    val c10 = ss.sqlContext.sql("select * from kmeans where prediction==1 and label==0")
    val cntC0 = c0.count().toDouble
    val cntC01 = c01.count().toDouble
    val cntC00 = cntC0 - cntC01
    val cntC1 = c1.count().toDouble
    val cntC11 = c11.count().toDouble
    val cntC10 = cntC1 - cntC11
    sampleWay match {
      case 0 =>
        import ss.implicits._
        val rate: Double = cntC01 / cntC00
        val negative = c00.rdd.map(x => (
          x(0).asInstanceOf[Double],
          x(1).asInstanceOf[Vector],
          x(2).asInstanceOf[Int])).sample(withReplacement = false, rate).toDF("label", "features", "prediction")
        val positive = c01
        val samples = positive.union(negative)
        samples.show(10)
        samples
      case 1 =>
        import ss.implicits._
        val rate: Double = cntC11 / cntC10
        val negative = c10.rdd.map(x => (
          x(0).asInstanceOf[Double],
          x(1).asInstanceOf[Vector],
          x(2).asInstanceOf[Int])).sample(withReplacement = false, rate).toDF("label", "features", "prediction")
        val positive = c11
        val samples = positive.union(negative)
        samples.show(10)
        samples
      case 2 =>
        val rate0: Double = cntC01 / cntC00
        val rate1: Double = cntC11 / cntC10
        val negativeC0 = c00.rdd.map(x => (
          x(0).asInstanceOf[Double],
          x(1).asInstanceOf[Vector],
          x(2).asInstanceOf[Int])).sample(withReplacement = false, rate0)
        val negativeC1 = c10.rdd.map(x => (
          x(0).asInstanceOf[Double],
          x(1).asInstanceOf[Vector],
          x(2).asInstanceOf[Int])).sample(withReplacement = false, rate1)
        import ss.implicits._
        val negative = negativeC0.union(negativeC1).toDF("label", "features", "prediction")
        val positive = c01.union(c11)
        val samples = positive.union(negative)
        samples.show(10)
        samples
    }
  }


  /**
   *
   * 3,070,855为1的样本 第0个簇有306,407个为1的样本 第1个簇有2,764,448个为1的样本
   * 43,384,058个为0的样本 第0个簇有6,786,926个为0的样本 第1个簇有第1个簇有36,597,132‬个为0的样本
   * 0样本是1样本的14倍
   * 第0个簇分数偏低,第1个簇分数偏高
   * 从训练数据中取样
   *
   * @param ss
   * @param sampleWay 采样方式 0:从0簇中取样 1:从1簇中取样 2:从两个簇中取样
   */
  def sample(ss: SparkSession, df: DataFrame, sampleWay: Int = -1): DataFrame = {
    def func(positive: DataFrame, numPositive: Double, negative: DataFrame, numNegative: Double): DataFrame = {
      if (numPositive > numNegative) {
        val rate = numNegative / numPositive
        negative.union(positive.sample(withReplacement = false, rate)).drop("prediction")
      } else {
        val rate = numPositive / numNegative
        negative.sample(withReplacement = false, rate).union(positive).drop("prediction")
      }
    }

    def avg(df: DataFrame, size: Double): Double = {
      import ss.implicits._
      val sum = df.select("features").map(row => {
        val values = row(0).asInstanceOf[SparseVector].values
        var i = 0
        var sum: Double = 0
        while (i < values.length) {
          if (i != 1) {
            sum += values(i)
            i += 1
          }
        }
        sum / values.length
      }).reduce(_ + _)
      sum / size
    }
    //    val df = ss.read.parquet(clusteredDataPath)
    ss.sqlContext.dropTempTable("kmeans")
    df.createTempView("kmeans")
    val c0 = ss.sqlContext.sql("select * from kmeans where prediction==0")
    val cntC0 = c0.count().toDouble
    c0.show(20, truncate = 0)
    val c1 = ss.sqlContext.sql("select * from kmeans where prediction==1")
    c1.show(20, truncate = 0)
    val cntC1 = c1.count().toDouble
    var k = 0
    //    val avg0 = avg(c0, cntC0)
    //    val avg1 = avg(c1, cntC1)
    //    println(s"avg0=$avg0\tavg1=$avg1")
    //    if (avg0 > avg1) {
    //      k = 0
    //    } else{
    //      k = 1
    //    }

    if (sampleWay == -1) {
      print("choose cluster: ")
      k = StdIn.readInt()
    } else {
      k = sampleWay
    }

    k match {
      case 0 =>
        val c01 = ss.sqlContext.sql("select * from kmeans where prediction==0 and label==1")
        val c00 = ss.sqlContext.sql("select * from kmeans where prediction==0 and label==0")
        val cntC01 = c01.count().toDouble
        val cntC00 = cntC0 - cntC01
        val samples = func(c01, cntC01, c00, cntC00)
        samples.show(10, truncate = 0)
        //        samples.write.save(sampledDataSavePath)
        samples
      case 1 =>
        val c11 = ss.sqlContext.sql("select * from kmeans where prediction==1 and label==1")
        val c10 = ss.sqlContext.sql("select * from kmeans where prediction==1 and label==0")
        val cntC11 = c11.count().toDouble
        val cntC10 = cntC1 - cntC11
        val samples = func(c11, cntC11, c10, cntC10)
        samples.show(10, truncate = 0)
        samples
      //        samples.write.save(sampledDataSavePath)
      case 2 =>
        val c01 = ss.sqlContext.sql("select * from kmeans where prediction==0 and label==1")
        val c00 = ss.sqlContext.sql("select * from kmeans where prediction==0 and label==0")
        val c11 = ss.sqlContext.sql("select * from kmeans where prediction==1 and label==1")
        val c10 = ss.sqlContext.sql("select * from kmeans where prediction==1 and label==0")
        val cntC01 = c01.count().toDouble
        val cntC00 = cntC0 - cntC01
        val cntC11 = c11.count().toDouble
        val cntC10 = cntC1 - cntC11
        val samples1 = func(c11, cntC11, c10, cntC10)
        val samples0 = func(c01, cntC01, c00, cntC00)
        val samples = samples1.union(samples0)
        samples.show(10, truncate = 0)
        //        samples.write.save(sampledDataSavePath)
        samples
    }
  }

  /**
   * 遍历所有作者的训练数据所在文件夹,将所有libsvm格式的文件组合成一个文件
   *
   * @param dirPath  目录位置
   * @param savePath 保存位置
   */
  def genLibsvm(dirPath: String, savePath: String): Unit = {
    var fileNum = 0
    val dir = new File(dirPath)
    if (dir.exists()) {
      val fout = new PrintWriter(savePath)
      val fileNameList = dir.listFiles()
      fileNum = fileNameList.length
      //list()方法是返回某个目录下的所有文件和目录的文件名，返回的是String数组
      //listFiles()方法是返回某个目录下所有文件和目录的绝对路径，返回的是File数组
      for (file <- fileNameList) {
        println(file.getName)
        val source = Source.fromFile(file)
        val lines = source.getLines()
        for (line <- lines) {
          fout.write(s"$line\n")
        }
        fout.flush()
      }
      fout.close()
      println(s"fileNum: $fileNum")
    }
    else System.out.println("directory path is invalid")
  }


  def genW2VModel(ss: SparkSession,
                  corpusPath: String,
                  w2vSavePath: String): Unit = {
    val corpus = loadCorpus(ss, textTxtPath = corpusPath)
    //    val corpus = loadCorpus(ss, textTxtPath = "d:/contest/text.txt", orgTxtPath ="d:/contest/org.txt", venueTxtPath = "d:/contest/venue.txt")
    trainWord2Vec(ss, corpus, modelSavePath = w2vSavePath)
  }

  def checkLRModel(ss: SparkSession): Unit = {
    loadLRModel("c:/users/jzy/desktop/lr") //从01两个簇中均匀采样训练的lr
    loadLRModel("c:/users/jzy/desktop/lr_nosample") //未采样的
    loadLRModel("c:/users/jzy/desktop/lr_sampled_from_cluster1") //仅从1簇采样的
  }

  def getPidsByName(authorRaw: JSONObject, name: String): Array[String] = {
    val aidPids = authorRaw.getJSONObject(name)
    val aids = authorRaw.getJSONObject(name).keySet().toArray[String](Array[String]())
    val allPids = ArrayBuffer[String]()
    var numAuthors = 0
    for (aid <- aids) {
      val pids = aidPids.getJSONArray(aid).toArray[String](Array[String]())
      if (pids.size >= 0) {
        allPids.appendAll(pids)
        numAuthors += 1
      }
    }
    allPids.toArray
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder
      .appName("feature regression")
      //      .config("spark.executor.memory", "6g")
      //      .master("local[1]") //设置运行环境为本地模式
      .getOrCreate()

    val startTime = System.currentTimeMillis //获取开始时间
    val w2vSavePath = "hdfs://datacsu1:9000/ad/word2vec"
    val corpusPath = "hdfs://datacsu1:9000/ad/corpus.txt"
    val corpus = loadCorpus(ss, textTxtPath = corpusPath)
    //    val corpus = loadCorpus(ss, textTxtPath = "d:/contest/text.txt", orgTxtPath ="d:/contest/org.txt",
    //    venueTxtPath = "d:/contest/venue.txt")
    trainWord2Vec(ss, corpus, vectorSize=100,windowSize=3,minCount=3,modelSavePath = w2vSavePath)
    //    genW2VModel(ss, corpusPath, w2vSavePath)
    //    val word2VecModel = ss.sparkContext.broadcast(Word2VecModel.load(Settings.w2vModelPath))
    //    val pubs = ss.sparkContext.broadcast(JsonUtil.loadJson(Settings.pubsJsonPath))
    //    val trainJson = ss.sparkContext.broadcast(JsonUtil.loadJson(Settings.trainJsonPath))
    //    val venuesDF = DataPreparation.getVenueDF(ss, Settings.venuesJsonPath,
    //      word2VecModel.value, 240).cache()

    //    val names = trainJson.value.keySet().toArray[String](Array[String]())
    //    var cnt = 1
    //    for (name <- names) {
    //      println(cnt, name)
    //      breakable {
    //        if (cnt > 100) break() else cnt += 1
    //        val training = AuthorNetwork.getDataForFeaturePrediction(ss,
    //          DataPreparation.prepare(ss, pubs.value, getPidsByName(trainJson.value, name),
    //            name, word2VecModel.value, venuesDF, 240))
    //        dumpDataForFeaturePrediction2Libsvm(ss, training, s"/root/${Settings.dataset}/fp", s"$name.txt")
    //      }
    //    }
    //    genLibsvm(s"/root/${Settings.dataset}/fp", s"/root/${Settings.dataset}/fp.txt")
    //    FileUtil.upload2hdfs(s"/root/${Settings.dataset}/fp.txt",s"${Settings.hdfsPrefix}/${Settings.dataset}/fp.txt")
    //    val training = ss.read.format("libsvm").option("numFeatures", "4").load(s"${Settings.hdfsPrefix}/${Settings.dataset}/fp.txt")
    //    val model = trainRegression(ss, training)
    //    model.save(s"${Settings.hdfsPrefix}/${Settings.dataset}/fp")
    val endTime = System.currentTimeMillis //获取结束时间
    System.out.println("Running Time: " + (endTime - startTime) / 1000 / 60 + "min")
    ss.stop()

  }
}
