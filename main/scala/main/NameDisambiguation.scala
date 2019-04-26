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
  val name = "xu_xu"
  val path = "d:/namedis/"

  def prepare(ss: SparkSession, vecSize: Int = 100): Unit = {
    val modelPath = path + s"/word2vec_$vecSize"
    val model = GlobalTraining.loadWord2VecModel(modelPath)
    //DataPreparation.prepareMLByName(ss, name, path, model)
    DataPreparation.prepareML(ss, name, path, model)
  }

  def train(ss: SparkSession, modelName: String) {

    val network = buildML(ss, path + name)
    val networkAfter = AuthorNetwork.runForTraining(network, numPartition)
    //networkAfter.cache()
    //AuthorNetwork.saveML(networkAfter, path + name)
    val trainingData = Training.getData(ss, networkAfter)

    //      trainingData.write.parquet(path + name + "/parquet")
    modelName match {
      case "lr" =>
        val model = Training.trainByLR(ss, trainingData)
        model.write.overwrite().save(path + modelName)
      case "lsvc" =>
        val model = Training.trainByLSVC(ss, trainingData)
        model.write.overwrite().save(path + modelName)
      case "mpc" =>
        val model = Training.trainByMPC(ss, trainingData)
        model.write.overwrite().save(path + modelName)
      case "nb" => val model = Training.trainByNB(ss, trainingData)
        model.write.overwrite().save(path + modelName)
      case _ => println("unknown model name")
    }
  }

  def disambiguate(ss: SparkSession, modelName: String = "skip"): Unit = {
    //生成初始网络
    def run(): Unit = {
      val network = buildML(ss, path + name)
      modelName match {
        case "lsvc" =>
          val model = LinearSVCModel.load(path + modelName)
          val networkAfter = AuthorNetwork.runML(network, numPartition, model)
          save(networkAfter, path + name)
        case "lr" =>
          val model = LogisticRegressionModel.load(path + modelName)
          val networkAfter = AuthorNetwork.runML(network, numPartition, model)
          save(networkAfter, path + name)
        case "mpc" =>
          val model = MultilayerPerceptronClassificationModel.load(path + modelName)
          val networkAfter = AuthorNetwork.runML(network, numPartition, model)
          save(networkAfter, path + name)
        case "skip" => println("skip")
        case _ => println("unknown model name")
      }
    }

    run()
    val graph = AnalysisTool.loadGraph(ss, path + name)

    def all(): Unit = {
      val file = Source.fromURL(this.getClass.getResource("/resources/100.txt"))
      val names = file.getLines().toArray
      val records = AnalysisTool.analyze(graph, names)
      val csv = new File("d:/namedis/100.csv")
      csv.createNewFile()
      CSVUtil.write(path + "100.csv", records = records)
    }

    def one(): Unit = {
      AnalysisTool.analyzeByName(graph, name)
    }

    one()
  }

  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val ss: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      .master("local[*]")
      .getOrCreate()
    prepare(ss)
    train(ss, "lr")
    disambiguate(ss, "lr")
    ss.stop()
  }
}
