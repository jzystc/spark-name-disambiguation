package util

import com.alibaba.fastjson
import com.alibaba.fastjson.JSONObject
import network.AuthorNetwork
import network.AuthorNetwork.{EdgeAttr, VertexAttr}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.Array.range
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * 数据准备
 */
object DataPreparation {

  /**
   * 从json文件中生成构建网络需要的parquet文件
   *
   * @param ss    SparkSession
   * @param pubs  json路径
   * @param model word2vec模型,用于转化字符串为词向量
   * @return Graph 返回文献网络对象
   */
  def prepare(ss: SparkSession, pubs: JSONObject, pids: Array[String], name: String, model: Word2VecModel,
              venuesDF: DataFrame, numPartitions: Int): Graph[VertexAttr, EdgeAttr] = {
    val vertexArray = new ArrayBuffer[(Long, String, String, String, String, Array[String], Int, String, String)]()
    //节点从1开始编号
    var vertexId = 1L
    //    遍历json中的每篇文章
    //    var cnt = 0
    for (paperId <- pids) {
      breakable {
        val paperJsonObj = pubs.getJSONObject(paperId)
        var text = paperJsonObj.getString("title")
        if (paperJsonObj.get("abstract") != null) {
          text = text + ". " + paperJsonObj.getString("abstract")
        }
        val year = paperJsonObj.getInteger("year").asInstanceOf[Int]
        val venue = paperJsonObj.getString("venue").trim
        val authorsJsonArr: fastjson.JSONArray = paperJsonObj.getJSONArray("authors")
        val coauthors = ArrayBuffer[String]()
        val iterator = authorsJsonArr.iterator
        var org = ""
        while (iterator.hasNext) {
          val author = iterator.next.asInstanceOf[fastjson.JSONObject]
          //          val authorName = author.getString("name").split('_').sorted.mkString("_")
          val authorName = author.getString("name")
          if (authorName != "") {
            coauthors.append(authorName)
            if (name.equals(authorName)) {
              org = author.getString("org")
            }
          }
          //          val sortedName = name.split('_').sorted.mkString("_")

        }
        val vertex = (vertexId, paperId, "", name, org, coauthors.toArray, year, text, venue)
        vertexArray.append(vertex)
        vertexId += 1
      }
    }
    val edgeRDD = generateEdges(ss, pids.length)
    //    val edgeRDD=ss.parallelize(edgeArray)
    var vertexDF = ss.createDataFrame(vertexArray).toDF("id", "paperId", "authorId", "name", "org",
      "coauthors", "year", "text", "venue")
    // vertexDF.repartition(numPartitions)
    //vertexDF中的org转换成向量
    vertexDF = TrainingUtil.transSentence2Vec(vertexDF, "org", "orgVec", model)
    //paperDF中的text转换成向量
    vertexDF = TrainingUtil.transSentence2Vec(vertexDF, "text", "textVec", model)
    // venuesDF.repartition(numPartitions)
    vertexDF = vertexDF.join(venuesDF,
      //条件1 作者名相同
      vertexDF("venue") === venuesDF("venue"), "left")
      //删除重复列
      .drop("venue")
    val vertexRDD = vertexDF2RDD(ss, vertexDF)
    Graph(vertexRDD, edgeRDD)
  }

  /**
   * @param numVertices number of vertices
   * @return edgeRDD
   */
  def generateEdges(ss: SparkSession, numVertices: Int): RDD[Edge[EdgeAttr]] = {
    val edgeArray = ArrayBuffer[Edge[EdgeAttr]]()
    val vids = range(1, numVertices + 1, 1)
    for (i <- (0 until numVertices - 2)) {
      for (j <- (i + 1 until numVertices - 1)) {
        edgeArray.append(Edge(vids(i).toLong, vids(j).toLong, (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)))
      }
    }
    val edgeRDD = ss.sparkContext.makeRDD(edgeArray)
    edgeRDD
  }

  def generateVertices(name: String, authorId: String, paperIds: Array[String], pubs: JSONObject): Unit = {

  }

  def prepareForTraining(ss: SparkSession, pubs: JSONObject, aidPids: JSONObject, name: String, model: Word2VecModel,
                         venuesDF: DataFrame, numPartitions: Int): Graph[VertexAttr, EdgeAttr] = {
    val vertexArray = new ArrayBuffer[(Long, String, String, String, String, Array[String], Int, String, String)]()
    //节点从1开始编号
    var vertexId = 1L
    //    遍历json中的每篇文章
    //    var cnt = 0
    val aids = aidPids.keySet().toArray[String](Array[String]())
    //    val listType = new TypeReference[Array[String]]() {}
    var numPapers = 0
    var cntSkip = 0
    var nSkipVenue = 0
    var nSkipOrg = 0
    var nSkipAuthor = 0
    for (aid <- aids) {
      //      println(aid)
      val pids = aidPids.getJSONArray(aid).toArray[String](Array[String]())
      //      val pids = aidPids.getJSONArray(aid).
      numPapers += pids.length
      //      println(aid, pids.length)
      for (paperId <- pids) {
        //        print(paperId)
        breakable {
          val paperJsonObj = pubs.getJSONObject(paperId)
          val title = paperJsonObj.getString("title")
          val paperAbstract = paperJsonObj.getString("abstract")
          val text = title + " " + paperAbstract
          val year = paperJsonObj.getInteger("year").asInstanceOf[Int]
          val venue = paperJsonObj.getString("venue").trim
          //skip paper without venue
          if ("".equals(venue)) {
            //            println("skip: venue is null")
            //            cntSkip += 1
            nSkipVenue += 1
            break()
          }
          val authorsJsonArr = paperJsonObj.getJSONArray("authors")
          //skip paper with only 1 author
          if (authorsJsonArr.size() <= 1) {
            //            println("skip: no author/only 1 author")
            //            cntSkip += 1
            nSkipAuthor += 1
            break()
          }
          val coauthors = ArrayBuffer[String]()
          val iterator = authorsJsonArr.iterator
          var org = ""
          while (iterator.hasNext) {
            val author = iterator.next.asInstanceOf[fastjson.JSONObject]
            //            val authorName = author.getString("name").toLowerCase.replace(".", "")
            //              .replace("-", "").replace(" ", "_")
            //            val authorName = author.getString("name").split('_').sorted.mkString("_")
            val authorName = author.getString("name")
            if (authorName != "") {
              coauthors.append(authorName)
              //              val sortedName = name.split('_').sorted.mkString("_")
              //              if (sortedName.equals(authorName.split('_').sorted.mkString("_"))) {
              //                //skip paper without author's affiliation
              //                              org = author.getString("org")
              //                              if ("".equals(org)) {
              //                                println("skip: org is null")
              //                                cntSkip += 1
              //                                break()
              //                              }
              //                            }
              if (name.equals(authorName)) {
                org = author.getString("org")
                if ("".equals(org)) {
                  //                  println("skip: org is null")
                  //                  cntSkip += 1
                  nSkipOrg += 1
                  break()
                }
              }
            }
          }

          val vertex = (vertexId, paperId, aid, name, org, coauthors.toArray, year, text, venue)
          //                println(vertex._5)
          vertexArray.append(vertex)
          vertexId += 1
        }
      }
    }
    println(s"skip $nSkipOrg: org is null")
    println(s"skip $nSkipAuthor: no author/only 1 author ")
    println(s"skip $nSkipVenue: venue is null")
    cntSkip = nSkipAuthor + nSkipOrg + nSkipVenue
    println(s"n_papers: $numPapers\t skip: $cntSkip")
    var vertexDF = ss.createDataFrame(vertexArray).toDF("id", "paperId", "authorId", "name", "org",
      "coauthors", "year", "text", "venue").repartition(numPartitions)
    //vertexDF中的org转换成向量
    vertexDF = TrainingUtil.transSentence2Vec(vertexDF, "org", "orgVec", model)
    //paperDF中的text转换成向量
    vertexDF = TrainingUtil.transSentence2Vec(vertexDF, "text", "textVec", model)
    vertexDF = vertexDF.join(venuesDF,
      //条件1 作者名相同
      vertexDF("venue") === venuesDF("venue"), "inner")
      //删除重复列
      .drop("venue")
    //    vertexDF.show(500)
    val vertexRDD = vertexDF2RDD(ss, vertexDF)
    //    vertexRDD.foreach(v => println(v._2._4 == null, v._2._6 == null, v._2._7 == null, v._2._8 == null))
    val edgeRDD = generateEdges(ss, numPapers - cntSkip).repartition(numPartitions)
    Graph(vertexRDD, edgeRDD)
  }

  /**
   * 将vertexDataFrame转换成rdd
   *
   * @param ss
   * @param vertexDF
   * @return
   */
  def vertexDF2RDD(ss: SparkSession, vertexDF: DataFrame, numPartitions: Int = 210): RDD[(VertexId, VertexAttr)] = {
    //    vertexDF.show(1)
    //   VertexAttr = (Name, AuthorId, OrgVec, PaperId, Year, TextVec, VenueVec)
    val vertexRDD = vertexDF.select("id", "paperId", "authorId", "name", "orgVec", "coauthors",
      "year", "textVec", "venueVec").rdd.repartition(numPartitions).map(x =>
      (
        //vertexId
        x(0) match {
          case i: Int => i.asInstanceOf[Int].toLong
          case j: Long => j.asInstanceOf[Long]
        },
        (
          //paperId
          x(1).asInstanceOf[String],
          //authorId
          {
            if (x(2) != null)
              x(2).asInstanceOf[String]
            else
              ""
          },
          //authorName
          x(3).asInstanceOf[String],
          //org
          x(4).asInstanceOf[Vector],
          x(5).asInstanceOf[Seq[String]].toArray,
          //发表年份
          x(6) match {
            case i: Long => i.asInstanceOf[Long].toInt
            case j: Int => j.asInstanceOf[Int]
          },
          //标题与摘要的向量
          x(7) match {
            case i: Seq[Double] => new DenseVector(i.asInstanceOf[Seq[Double]].toArray)
            case j: Vector => j.asInstanceOf[Vector]
          },
          //会议/期刊名对应的向量
          x(8) match {
            case i: Seq[Double] => new DenseVector(i.asInstanceOf[Seq[Double]].toArray)
            case j: Vector => j.asInstanceOf[Vector]
          }
        )
      ))
    vertexRDD
  }

  def edgeDF2RDD(ss: SparkSession, edgeDF: DataFrame): RDD[Edge[EdgeAttr]] = {
    val edgeRDD = edgeDF.select("srcId", "dstId", "label", "orgSim", "coauthorSim",
      "textSim", "yearSim", "venueSim").rdd.map(x =>
      //srcId,源节点id,类型为Long
      Edge(
        x(0) match {
          case i: Int => i.asInstanceOf[Int].toLong
          case j: Long => j.asInstanceOf[Long]
        },
        //dstId,目的节点id,类型为Long
        x(1) match {
          case i: Int => i.asInstanceOf[Int].toLong
          case j: Long => j.asInstanceOf[Long]
        },
        //attributes
        (
          //label
          x(2).asInstanceOf[Double],
          //orgSim
          x(3).asInstanceOf[Double],
          //coauthorSim
          x(4).asInstanceOf[Double],
          //textSim
          x(5).asInstanceOf[Double],
          //yearSim
          x(6).asInstanceOf[Double],
          //venueSim
          x(7).asInstanceOf[Double]
        )
      )
    )
    edgeRDD
  }

  def getVenueDF(ss: SparkSession, venueTextPath: String, w2vModel: Word2VecModel, numPartitions: Int): DataFrame = {
    //    val schema = StructType(List(
    //      StructField("venue", StringType, nullable = false),
    //      StructField("text", StringType, nullable = false)
    //    ))
    val df = ss.read.format("json").load(venueTextPath)
    val venueDF = TrainingUtil.transSentence2Vec(df, "text", "venueVec", model = w2vModel)
    venueDF
  }


  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val ss: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      .config("spark.executor.memory", "6g")
      .master("local[1]")
      .getOrCreate()
    val word2VecModel = Word2VecModel.load("D:/vmshare/word2vec_100")

    //    //    val jsonPath = "D:\\Users\\jzy\\Documents\\PycharmProjects\\dataprocessing\\author-disambiguation\\result2.json"
    val pubsJsonPath = "D:/sigir2020/kdd/clean_pubs.json"

    val pubs = JsonUtil.loadJson(pubsJsonPath)
    val name = "huibin_xu"
    val aidPids = JsonUtil.loadJson("d:/sigir2020/kdd/name_train_500.json").getJSONObject(name)

    //    val venueJsonPath = "D:/na-contest/venues.json"
    //    //    val savePath = "D:\\Users\\jzy\\Documents\\PycharmProjects\\dataprocessing\\author-disambiguation"
    //    val savePath = "D:/na-contest/sna"
    //    //    val name = "test_pubs"
    //    val pubs = JsonUtil.loadJson(pubsJsonPath)
    //    val venues=JsonUtil.loadJson(venueJsonPath)
    //    prepare(ss, pubs, name, word2VecModel, venueJsonPath, 30)\
    val venueTextPath = "d:/sigir2020/kdd/clean_venues.json"
    //    val df = ss.read.format("json").load(venueTextPath)
    val venueDF = getVenueDF(ss, venueTextPath, word2VecModel, 1)
    val graph = DataPreparation.prepareForTraining(ss, pubs, aidPids, name, word2VecModel, venueDF, 1)
    //        val trainingData = AuthorNetwork.trainingTextAndVenue(graph)
    val trainingData = AuthorNetwork.training(ss, graph)
    print(trainingData.count())
    //    import ss.implicits._
    //    val trainingDataDF = trainingData.map(x => edge(x.attr._1, Seq[Double](x.attr._2, x.attr._3, x.attr._4, x.attr._5, x.attr._6))).toDF()
    //    trainingDataDF.show()
    ss.close()
  }
}
