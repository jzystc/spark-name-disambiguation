package main

import java.io.File

import main.AuthorNetwork.{buildML, save}
import org.apache.spark.ml.classification._
import org.apache.spark.sql.SparkSession
import util._

import scala.io.Source

object NameDisambiguation {
  //最大迭代次数
  var maxIterations = 10
  //收敛容忍度
  var tolerance = 0.1
  //rdd分区数
  var numPartition = 300

  //数据库表名
  var name = "hongbin_liang"

  var path = "d:/namedis/"

  def prepare(ss: SparkSession, vecSize: Int = 100): Unit = {
    val modelPath = path + s"/word2vec/word2vec_$vecSize"
    val model = GlobalTraining.loadWord2VecModel(modelPath)
    //DataPreparation.prepareMLByName(ss, name, path, model)
    DataPreparation.prepareML(ss, name, path, model)
  }

  def train(ss: SparkSession, modelName: String, maxIter: Int) {

    val network = buildML(ss, path + name)
    val networkAfter = AuthorNetwork.runForTraining(network, numPartition)
    //networkAfter.cache()
    //AuthorNetwork.saveML(networkAfter, path + name)
    val trainingData = Training.getData(ss, networkAfter)

    //      trainingData.write.parquet(path + name + "/parquet")
    modelName match {
      case "lr" =>
        val model = Training.trainByLR(ss, trainingData, maxIter)
        model.write.overwrite().save(path + modelName)
      case "lsvc" =>
        val model = Training.trainByLSVC(ss, trainingData, maxIter)
        model.write.overwrite().save(path + modelName)
      case "mpc" =>
        val model = Training.trainByMPC(ss, trainingData, maxIter)
        model.write.overwrite().save(path + modelName)
      case "nb" => val model = Training.trainByNB(ss, trainingData)
        model.write.overwrite().save(path + modelName)
      case _ => println("unknown model name")
    }
  }

  def disambiguate(ss: SparkSession, modelName: String = "lr", csvPath: String = "/root/100.csv"): Unit = {
    println(s"csvPath: $csvPath")
    //生成初始网络
    modelName match {
      case "lsvc" =>
        val network = buildML(ss, path + name)
        val model = LinearSVCModel.load(path + modelName)
        val networkAfter = AuthorNetwork.runML(network, numPartition, model)
        save(networkAfter, path + name)
      case "lr" =>
        val network = buildML(ss, path + name)
        val model = LogisticRegressionModel.load(path + modelName)
        val networkAfter = AuthorNetwork.runML(network, numPartition, model)
        save(networkAfter, path + name)
      case "mpc" =>
        val network = buildML(ss, path + name)
        val model = MultilayerPerceptronClassificationModel.load(path + modelName)
        val networkAfter = AuthorNetwork.runML(network, numPartition, model)
        save(networkAfter, path + name)
      case "skip" => println("skip")
      case _ => println("unknown model name")
    }
    val graph = AnalysisTool.loadGraph(ss, path + name)

    def all(): Unit = {
      val file = Source.fromURL(this.getClass.getResource("/resources/100.txt"))
      val names = file.getLines().toArray
      val records = AnalysisTool.analyze(graph, names)
      val csv = new File(csvPath)
      csv.createNewFile()
      CSVUtil.write(csvPath, records = records)
    }

    def one(): Unit = {
      AnalysisTool.analyzeByName(graph, name)
    }

    if (name.equals("all") || name.equals("test")) {
      all()
    } else {
      one()
    }
  }

  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val ss: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      //.master("local[*]")
      .getOrCreate()

    if (args.contains("-n")) {
      name = args(args.indexOf("-n") + 1)
      println(s"name: $name")
    } else {
      println("please specify name. eg: -n test")
      ss.stop()
    }
    if (args.contains("-P")) {
      path = args(args.indexOf("-P") + 1)
      println(s"path: $path")
    }
    if (args.contains("-nP")) {
      numPartition = args(args.indexOf("-nP") + 1).toInt
      println(s"numPartition: $numPartition")
    }
    var modelName = "lr"
    var maxIter = 10
    if (args.contains("-M")) {
      modelName = args(args.indexOf("-M") + 1)
      println(s"modelName: $modelName")
      if (args.contains("-i")) {
        maxIter = args(args.indexOf("-i") + 1).toInt
      }
      println(s"maxIter: $maxIter")
    }
    var vecSize = 100
    if (args.contains("-v")) {
      vecSize = args(args.indexOf("-v") + 1).toInt
      println(s"vecSIze: $vecSize")
    }
    if (args.contains("-m")) {
      val mode = args(args.indexOf("-m") + 1)
      if (mode.contains("p")) {
        prepare(ss, vecSize)
      }
      if (mode.contains("t")) {
        train(ss, modelName, maxIter)
      }
      if (mode.contains("d")) {
        if (args.contains("-csv")) {
          val csvPath = args(args.indexOf("-csv") + 1)
          disambiguate(ss, modelName, csvPath)
        } else {
          disambiguate(ss, modelName)
        }
      } else {
        println("wrong params for running mode.")
        ss.stop()
      }
    } else {
      println("please specify running mode. eg: -m ptd")
      ss.stop()
    }
    ss.stop()
  }
}
