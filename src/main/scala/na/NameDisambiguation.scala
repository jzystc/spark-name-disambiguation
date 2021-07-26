package na

import java.io.{File, FileWriter, IOException}

import _root_.util.{DataPreparation, FileUtil, JsonUtil, TrainingUtil}
import com.alibaba.fastjson.JSONObject
import na.AuthorNetwork._
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.Settings._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks._

/**
 * main entrance
 */
object NameDisambiguation {

  def isAbbrName(name: String): Boolean = {
    val parts = name.split("_")
    parts.foreach(x => if (x.length == 1) {
      true
    })
    false
  }

  case class edge(label: Double, features: Seq[Double])

  /**
   * 生成每个名字的训练数据
   * generate training data(libsvm format) of per name
   *
   * @param ss SparkSession
   */
  def genTrainingData(ss: SparkSession, w2vModelPath: String, pubsJsonPath: String, trainJsonPath: String,
                      venuesJsonPath: String, libsvmSavePath: String,
                      balance: Boolean = true, numPartitions: Int = 360, maxNameNum: Int = 0, maxAuthorNum: Int = 0): Unit = {
    val word2VecModel = ss.sparkContext.broadcast(Word2VecModel.load(w2vModelPath))
    val pubs = ss.sparkContext.broadcast(JsonUtil.loadJson(pubsJsonPath))
    val trainJson = ss.sparkContext.broadcast(JsonUtil.loadJson(trainJsonPath))
    //    val venues = ss.sparkContext.broadcast(JsonUtil.loadJson(venuesJsonPath))
    val keys = trainJson.value.keySet().iterator()
    val file = new File(s"/root/$dataset/trained_names.txt")
    if (!file.exists()) {
      file.createNewFile()
    }
    val allNames = ArrayBuffer[String]()
    while (keys.hasNext) {
      allNames.append(keys.next())
    }
    val source = Source.fromFile(file)
    //    val file = Source.fromURL(this.getClass.getResource(s"/disambiguated_names.txt"))
    val trainedNames = source.getLines()
    val names = allNames.toSet -- trainedNames.toSet
    val venuesDF = DataPreparation.getVenueDF(ss, venuesJsonPath, word2VecModel.value, numPartitions).cache()
    //    venuesDF.show(3)
    var cnt = 1
    var cntSkip = 0
    for (name <- names) {
      breakable {
        if (cnt > maxNameNum && maxNameNum != 0) {
          return
        }
        println(cnt, name)
        val aidPids = trainJson.value.getJSONObject(name)
        if (aidPids.isEmpty) {
          println("skip: empty")
          break()
        }
        val numEntities = aidPids.keySet().size()
        if (numEntities > maxAuthorNum && maxAuthorNum != 0) {
          println(s"skip $name")
          cntSkip += 1
          break()
        }

        val graph = DataPreparation.prepareForTraining(ss, pubs.value, aidPids, name, word2VecModel.value, venuesDF, numPartitions)
        //        val trainingData = AuthorNetwork.trainingTextAndVenue(graph)
        println("train...")
        val trainRDD = AuthorNetwork.training(ss, graph)
        //        val sqlContext= new org.apache.spark.sql.SQLContext(ss.sqlContext)
        import ss.sqlContext.implicits._
        println("balance...")
        var train = trainRDD.toDF("label", "features")
        if (balance) {
          train = TrainingUtil.balance(ss, train)
        }

        //        println("[clustering]")
        //        val groupedData = TrainingUtil.groupData(ss, trainingDataDF, numPartitions)
        //        ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
        //        val sampledData = TrainingUtil.sampling(ss, groupedData, 2)
        println(s"dump to $name.txt...")
        TrainingUtil.dumpDF2Libsvm(ss, train, s"$libsvmSavePath", s"$name.txt")
        ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
        //记录已训练的名字
        try {
          val file = new File(s"/root/$dataset/trained_names.txt")
          if (!file.exists()) {
            file.createNewFile()
          }
          val fileWriter = new FileWriter(file, true);
          fileWriter.write(s"$name\n")
          fileWriter.close()
          cnt += 1
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
    println(s"skip $cntSkip names")
  }

  def disambiguateByName(ss: SparkSession, numPartitions: Int = 360, w2vModelPath: String,
                         pubsJsonPath: String, testJsonPath: String, venuesJsonPath: String, modelPath: String,
                         name: String, threshold: Double = 0.5, simSavePath: String): Unit = {
    val word2VecModel = Word2VecModel.load(w2vModelPath)
    val clf = LogisticRegressionModel.load(modelPath)
    clf.setThreshold(threshold)
    //      classificationModel.setThreshold(0.6)
    //      val rfPath = "/user/root/contest/rf"
    //      val classificationModel = RandomForestClassificationModel.load(rfPath)
    val pubs = JsonUtil.loadJson(pubsJsonPath)
    val testJson = JsonUtil.loadJson(testJsonPath)
    //    val venues = JsonUtil.loadJsonArray(venuesJsonPath)
    //    val names = ArrayBuffer[String]()
    //    while (keys.hasNext) {
    //      names.append(keys.next())
    //    }
    //    val listType = new TypeReference[Array[String]]() {}
    val venueDF = DataPreparation.getVenueDF(ss, venuesJsonPath, word2VecModel, numPartitions)
    //    if (isAbbrName(name)) {
    //      classificationModel.asInstanceOf[LogisticRegressionModel].setThreshold(thresholdForAbbrName)
    //    } else {
    //      classificationModel.asInstanceOf[LogisticRegressionModel].setThreshold(thresholdForNormalName)
    //    }
    val aidPids = testJson.getJSONObject(name)
    val aids = testJson.getJSONObject(name).keySet().toArray[String](Array[String]())
    val allPids = ArrayBuffer[String]()
    for (aid <- aids) {
      val pids = aidPids.getJSONArray(aid).toArray[String](Array[String]())
      allPids.appendAll(pids)
    }
    println(name, allPids.length, aids.size)
    val graph = DataPreparation.prepare(ss, pubs, allPids.toArray, name, word2VecModel, venueDF, numPartitions)
    dumpProbability(ss, graph, clf, simSavePath, name, numPartitions)
    //      val disambiguatedGraph = AuthorNetwork.disambiguate(initGraph = graph, model = clf.value, numPartitions = numPartitions)
    //    val disambiguatedGraph = AuthorNetwork.disambiguate2(initGraph = graph, model = classificationModel,
    //      numPartitions = numPartitions)
    //val nameResult = new JSONObject()
    //    val result = getDisambiguationResult(disambiguatedGraph)
    //    //    ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
    //    println(s"n_cluster: ${result.length}")
    //    nameResult.put(name, result)
    //    JsonUtil.saveJson(nameResult, s"$singleResultSavePath/$name.json")
  }

  /**
   * 对每个名字进行消歧处理
   * 读入文件
   */
  def disambiguateAll(ss: SparkSession, numPartitions: Int = 360, w2vModelPath: String,
                      pubsJsonPath: String, testJsonPath: String, venuesJsonPath: String,
                      disambiguatedNameTxtPath: String, modelPath: String, modelType: String = "lr", threshold: Double = 0.5): Unit = {
    val word2VecModel = ss.sparkContext.broadcast(Word2VecModel.load(w2vModelPath))
    val pubs = ss.sparkContext.broadcast(JsonUtil.loadJson(pubsJsonPath))
    val testJson = ss.sparkContext.broadcast(JsonUtil.loadJson(testJsonPath))
    val clf = ss.sparkContext.broadcast(LogisticRegressionModel.load(modelPath))
    if (modelType.equals("lr")) {
      println(s"coefficients: ${clf.value.coefficients}")
      println(s"intercept: ${clf.value.intercept}")
      clf.value.setThreshold(threshold)
    }

    val allNames = testJson.value.keySet().toArray[String](Array[String]())
    //    val file = Source.fromURL(this.getClass.getResource(s"/disambiguated_names.txt"))
    val file = new File(disambiguatedNameTxtPath)
    if (!file.exists()) {
      file.createNewFile()
    }
    val source = Source.fromFile(file)
    //    val file = Source.fromURL(this.getClass.getResource(s"/disambiguated_names.txt"))
    val disambiguatedNames = source.getLines()
    val names = allNames.toSet -- disambiguatedNames.toSet
    //    val numNames = names.size
    //    val names = ArrayBuffer[String]()
    //    while (keys.hasNext) {
    //      names.append(keys.next())
    //    }

    val venuesDF = DataPreparation.getVenueDF(ss, venuesJsonPath, word2VecModel.value, numPartitions)
    var cnt = 1
    //    val clf = RandomForestClassificationModel.load(modelPath)
    //    val clf = LinearSVCModel.load(modelPath)
    for (name <- names) {
      breakable {
        val aidPids = testJson.value.getJSONObject(name)
        // if(aidPids.isEmpty || aidPids.size==0){
        // println("skip: empty")
        //break()
        //}
        val aids = testJson.value.getJSONObject(name).keySet().toArray[String](Array[String]())
        val allPids = ArrayBuffer[String]()
        var numAuthors = 0
        for (aid <- aids) {
          val pids = aidPids.getJSONArray(aid).toArray[String](Array[String]())
          if (pids.size >= 0) {
            allPids.appendAll(pids)
            numAuthors += 1
          }
        }
        println(cnt, name, allPids.length, numAuthors)
        cnt += 1
        //      if (isAbbrName(name)) {
        //        clf.asInstanceOf[LogisticRegressionModel].setThreshold(thresholdForAbbrName)
        //      } else {
        //        clf.asInstanceOf[LogisticRegressionModel].setThreshold(thresholdForNormalName)
        //      }
        //      classificationModel.asInstanceOf[LogisticRegressionModel].setThreshold(0.7)
        val nameResult = new JSONObject()
        //      val pids = namePids.getObject(name, listType).asInstanceOf[Array[String]]

        val graph = DataPreparation.prepare(ss, pubs.value, allPids.toArray, name, word2VecModel.value, venuesDF, numPartitions)
        //      val disambiguatedGraph = AuthorNetwork.disambiguate2(initGraph = graph, model = clf,
        //        numPartitions = numPartitions)
        println("disambiguate...")
        //        dumpProbability(ss, graph, clf.value, simSavePath, name, numPartitions)
        val disambiguatedGraph = AuthorNetwork.disambiguate(initGraph = graph, model = clf.value, numPartitions = numPartitions)
        val result = getDisambiguationResult(disambiguatedGraph)
        println(s"n_cluster: ${result.length}")
        nameResult.put(name, result)
        println("dump result...")
        JsonUtil.saveJson(nameResult, s"$resultSavePath/$name.json")
        ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
        try {
          val file = new File(disambiguatedNameTxtPath)
          if (!file.exists()) {
            file.createNewFile()
          }
          val fileWriter = new FileWriter(file, true)
          fileWriter.write(s"$name\n");
          fileWriter.close();
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }

  }

  /**
   * 训练逻辑回归模型
   * train a logistics regression model
   * load libsvm.txt first
   *
   * @param ss
   */
  def trainClf(ss: SparkSession, data: DataFrame, modelSavePath: String, modelType: String = "lr"): Unit = {
    // Scala
    def train(df: DataFrame): Unit = {
      modelType match {
        case "lr" => val lrModel = TrainingUtil.trainLR(ss, df)
          lrModel.write.overwrite.save(modelSavePath)
        case "rf" => val rfModel = TrainingUtil.trainRandomForest(ss, df)
          rfModel.write.overwrite.save(modelSavePath)
        case "svm" => val svmModel = TrainingUtil.trainSVM(ss, df)
          svmModel.write.overwrite.save(modelSavePath)
      } //    val df = ss.read.parquet(trainDataPath)
    }

    train(data)
  }


  def main(args: Array[String]): Unit = {
    // 初始化SparkSession
    val ss: SparkSession = SparkSession.builder()
      .appName("Author Disambiguation")
      //      .config("spark.sql.broadcastTimeout", "36000")
      // 若在本地运行需要设置为local
      //      .config("spark.executor.memory", "6g")
      .master("local[*]")
      .getOrCreate()
    val startTime = System.currentTimeMillis //获取开始时间
    genTrainingData(ss, w2vModelPath, pubsJsonPath, trainJsonPath, venuesJsonPath, libsvmSavePath,
      true, 360, 100, 0)
    ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
    println("generate libsvm.txt...")
    TrainingUtil.genLibsvm(libsvmSavePath, unionLibsvmPath)
    FileUtil.upload2hdfs(unionLibsvmPath, s"$hdfsPrefix/$dataset/")
    ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
    println("clustering...")
    val clusteredData = TrainingUtil.clusterData(ss, unionLibsvmHdfsPath, 360)
    //    ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
    println("sampling...")
    val samples = TrainingUtil.sample(ss, clusteredData, -1)
    //    val samples = ss.read.format("libsvm").option("numFeatures", "5").load(unionLibsvmHdfsPath).repartition(360)
    ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
    println("train classifier...")
    trainClf(ss, samples, modelPath, "lr")
    ss.sparkContext.getPersistentRDDs.foreach(x => x._2.unpersist())
    disambiguateAll(ss, 360, w2vModelPath,
      pubsJsonPath, testJsonPath, venuesJsonPath,
      disambiguatedNameTxtPath = s"/root/$dataset/disambiguated_names2.txt", modelPath, "lr", 0.5)
    //    disambiguateByName(ss, 3, "d:/sigir2020/word2vec/word2vec_100",
    //      "d:/sigir2020/kdd/global/clean_pubs.json", "d:/sigir2020/kdd/name_test_100.json",
    //      "d:/sigir2020/kdd/clean_venues.json", "d:/sigir2020/kdd/clf/lr",
    //      "song_chen", 0.9,"d:/")
    val endTime = System.currentTimeMillis //获取结束时间
    System.out.println("Running Time: " + (endTime - startTime) / 1000 / 60 + "min")
    ss.stop()
  }
}

