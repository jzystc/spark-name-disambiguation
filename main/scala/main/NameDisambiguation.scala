package main

import main.AuthorNetwork.{buildML, save}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import util._

import scala.io.Source

object NameDisambiguation {
  //最大迭代次数
  var maxIterations = 10
  //收敛容忍度
  var tolerance = 0.1
  //rdd分区数
  var numPartition = 200

  //数据库表名
  val name = "j_yu"
  val path = "d:/namedis/"

  def prepare(ss: SparkSession): Unit = {
    val modelPath = "d:/namedis/word2vec/word2vec_100"
    val model = GlobalTraining.loadWord2VecModel(modelPath)
    //DataPreparation.prepareMLByName(ss, name, path, model)
    DataPreparation.prepareML(ss, name, path, model)
  }

  def train(ss: SparkSession) {

    val network = buildML(ss, path + name)
    val networkAfter = AuthorNetwork.runForTraining(network, numPartition)
    //networkAfter.cache()
    //AuthorNetwork.saveML(networkAfter, path + name)
    val trainingData = Training.getData(ss, networkAfter)
    //      trainingData.write.parquet(path + name + "/parquet")
    val lrModel = Training.trainByLR(ss, trainingData)
    //val mpcModel = Training.trainByMPC(ss, trainingData)
    lrModel.write.overwrite().save("d:/namedis/lr")
    //mpcModel.write.overwrite().save("/home/csubigdata/namedis/mpc")

  }

  def disambiguate(ss: SparkSession): Unit = {
    //生成初始网络

    val network = buildML(ss, path + name)
    val lrModel = LogisticRegressionModel.load("d:/namedis/lr")
    // val mpcModel = MultilayerPerceptronClassificationModel.load("/home/csubigdata/namedis/mpc")
    val networkAfter = AuthorNetwork.runML(network, numPartition, lrModel)
    save(networkAfter, path + name)
    val graph = AnalysisTool.loadGraph(ss, path + name)
    def all(): Unit ={
          val file = Source.fromURL(this.getClass.getResource("/resources/100.txt"))
          val names = file.getLines().toArray
          val records = AnalysisTool.analyze(graph, names)
          CSVUtil.write(path + "100.csv", records = records)
    }

    AnalysisTool.analyzeByName(graph, name)
  }

  def main(args: Array[String]): Unit = {

    //初始化SparkSession
    val ss: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      .master("local[*]")
      .getOrCreate()
    prepare(ss)
    //train(ss)
    disambiguate(ss)
    ss.stop()
  }
}
