package network

import java.io.{File, FileWriter, PrintWriter}
import java.util

import _root_.util.SimilarityUtil._
import _root_.util.{AnalysisUtil, _}
import com.alibaba.fastjson.{JSONObject, TypeReference}
import org.apache.spark.graphx._
import org.apache.spark.ml.classification.{ClassificationModel, LogisticRegressionModel}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object AuthorNetwork {
  /**
   * 定义边属性的数据类型
   * 1. 2.0:合作者关系,1.0:link边两端节点已消歧,0-1:link边两端节点的相似度
   * 2 link边两端节点的文本和机构相似分数
   * 3 link边两端节点的层次(发表年份)缩放系数
   **/
  type YearSim = Double
  type Label = Double
  type OrgSim = Double
  type TextSim = Double
  type CoauthorSim = Double
  type VenueSim = Double

  /**
   * 定义用于训练参数的边属性的数据类型
   * 1.lable(0: id相同 1: id不同)
   * 2.机构相似度
   * 3.合作者相似度 重名合作者的平均机构相似分数
   * 4.标题与摘要的文本相似度
   * 5.发表年份相似度
   **/
  type EdgeAttr = (Label, OrgSim, CoauthorSim, TextSim, YearSim, VenueSim)
  type EdgeAttrWithoutYearSim = (Label, OrgSim, CoauthorSim, TextSim, VenueSim)

  /**
   * 定义节点属性的数据类型
   * 1.名字
   * 2.作者id
   * 3.机构向量
   * 4.发表年份
   * 5.标题向量val v1 = new DenseVector[Double](v1)
   * 6.摘要向量
   */
  type Name = String
  type AuthorId = String
  type OrgVec = Vector
  type PaperId = String
  type Year = Int
  type TextVec = Vector
  type Venue = String
  type Org = Array[String]
  type VenueVec = Vector
  type VertexAttr = (PaperId, AuthorId, Name, OrgVec, Array[Name], Year, TextVec, VenueVec)

  /**
   * 生成评测参照集
   * 元素: (srcId,dstId,name)
   *
   * @param g    作者-文献网络
   * @param path 保存位置
   * @return
   */
  def runForAnalysis(g: Graph[VertexAttr, EdgeAttr], path: String): RDD[(VertexId, VertexId, Name)] = {
    val graph = g.mapTriplets(triplet => {
      //边的label为2时表示合作者关系,这里只计算label为1的link边
      if (triplet.attr._1 != 2.0) {
        if (triplet.srcAttr._2.equals(triplet.dstAttr._2)) {
          (1.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        } else {
          (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        }
      } else
        triplet.attr
    })
    val pairs = graph.triplets.filter(_.attr._1 == 1).map(t => (t.srcId, t.dstId, t.srcAttr._1))
    pairs.saveAsObjectFile(s"$path/truepairs")
    pairs
  }

  def getJsonForEvaluationFromNetwork(g: Graph[VertexAttr, EdgeAttr], names: Array[String]): JSONObject = {
    val jsonObject = new JSONObject()
    for (name <- names) {
      val vertices = g.vertices.filter(v =>
        v._2._1.equals(name))
      //      vertices.foreach(x=>println(x._1,x._2._1,x._2._2))
      val authorIdVertexIdRDD = vertices.groupBy(_._2._2)
      //      rdd_authorLabelVertexId.foreach(println(_))
      val result = authorIdVertexIdRDD.map(x => {
        x._2.map(y => y._1).toArray
      }
      ).collect()
      //      result.foreach(x => {
      //        print("[")
      //        x.foreach(y => print(s"$y "))
      //        println("]")
      //      })
      jsonObject.put(name, result)
    }
    jsonObject
  }

  /**
   * 读取用于训练逻辑回归模型的作者-文献网络
   *
   * @param ss   sparkSession
   * @param path 作者-文献网络的保存位置
   * @return
   */
  def buildForTraining(ss: SparkSession, path: String): Graph[VertexAttr, EdgeAttr] = {
    val vIn = s"$path/ml_input_v"
    val eIn = s"$path/ml_input_e"
    val vertexRDD = ss.sparkContext.objectFile[(VertexId, VertexAttr)](vIn)
    val edgeRDD = ss.sparkContext.objectFile[Edge[EdgeAttr]](eIn)
    val network = Graph(vertexRDD, edgeRDD)
    network
  }

  /**
   * 两两组合名字相同节点对应的vIds
   *
   * @param vIds vertexId 数组
   * @return
   */
  def combine(vIds: Array[Long]): Set[(Long, Long)] = {
    var result = Set[(Long, Long)]()
    for (i <- vIds.indices) {
      for (j <- vIds.length - 1 to i + 1 by -1) {
        result += ((vIds(i), vIds(j)))
      }
    }
    result
  }


  /**
   * 读取用于重名作者去歧的作者文献网络
   *
   * @param ss   sparkSession
   * @param path 作者-文献网络保存位置 vertexRDD和edgeRDD
   * @return network
   */
  def build(ss: SparkSession, path: String): Graph[VertexAttr, EdgeAttr] = {
    val vIn = s"$path/input_v"
    val eIn = s"$path/input_e"
    val vertexRDD = ss.sparkContext.objectFile[(VertexId, VertexAttr)](vIn)
    val edgeRDD = ss.sparkContext.objectFile[Edge[EdgeAttr]](eIn)
    val network = Graph(vertexRDD, edgeRDD)
    network
  }

  /**
   * 构建输入到GAE的图,vertices源自原始图,edegs源自消歧后的图
   *
   * @param ss           SparkSession
   * @param verticesPath vertexRDD路径
   * @param edgesPath    edgeRDD路径
   * @return
   */
  def buildForGAE(ss: SparkSession, verticesPath: String, edgesPath: String, numPartitions: Int): Graph[(String, String, String), Double] = {
    val initialVertices = ss.sparkContext.objectFile[(VertexId, VertexAttr)](verticesPath)
    initialVertices.repartition(numPartitions)
    initialVertices.cache()
    val vertices = initialVertices.map(v => (v._1, (v._2._1, v._2._2, v._2._3)))
    val edges = ss.sparkContext.objectFile[Edge[Double]](edgesPath)
    val graph = Graph(vertices, edges)
    graph
  }

  def genVertexIdAuthorIdPaperIdJson(graph: Graph[(String, String, String), Double]): JSONObject = {
    val vertices = graph.vertices.collect()
    val jsonObj = new JSONObject()
    for (v <- vertices) {
      val attr = new JSONObject()
      attr.put("name", v._2._1)
      attr.put("author_id", v._2._2)
      attr.put("paper_id", v._2._3)
      jsonObj.put(v._1.toString, attr)
    }
    jsonObj
  }

  def dumpEdgesForGAE(graph: Graph[(String, String, String), Double], names: Array[String]): JSONObject = {
    val triplets = graph.triplets.filter(t => t.attr == 1)
    triplets.persist()
    val listType = new TypeReference[util.ArrayList[Array[Long]]]() {}
    val result = triplets.map(t => (t.srcAttr._1, (t.srcId, t.dstId))).collect()
    val json = new JSONObject()
    result.foreach(t => if (names.contains(t._1)) {
      if (json.get(t._1) != null) {
        val list = json.getObject(t._1, listType).asInstanceOf[util.ArrayList[Array[Long]]]
        list.add(Array[Long](t._2._1, t._2._2))
        json.put(t._1, list)
      }
      else {
        val list = new util.ArrayList[Array[Long]]()
        list.add(Array[Long](t._2._1, t._2._2))
        json.put(t._1, list)
      }
    })
    json
  }

  /**
   * 获取Option中的Set数据
   *
   * @param x 待处理的Option数据
   * @return
   */
  private def getSetFromOption(x: Option[Set[(String, Vector)]]) = x match {
    case Some(s) => s
    case None => Set[(String, Vector)]()
  }


  /**
   * 获得用于训练模型的图
   *
   * @param initGraph 作者网络
   */
  def training(ss: SparkSession, initGraph: Graph[VertexAttr, EdgeAttr]): RDD[(Double, Seq[Double])] = {
    //第一次更新link分数后的边数组
    val graphAfterFirstComputation = initGraph.mapTriplets(triplet => {
      //边的属性为2时表示合作者关系 只对link边计算
      //author_id相同且不等于0(0表示缺省)则返回1(1表示合并节点),否则返回文本相似度
      val orgSim = cosine(triplet.srcAttr._4, triplet.dstAttr._4)
      val coauthorSim = jaccard(triplet.srcAttr._5, triplet.dstAttr._5)
      //      val yearSim = computeYearSim(triplet.srcAttr._6, triplet.dstAttr._6)
      val yearSim = 0.0
      val textSim = cosine(triplet.srcAttr._7, triplet.dstAttr._7)
      val venueSim = cosine(triplet.srcAttr._8, triplet.dstAttr._8)
      if (triplet.srcAttr._2.equals(triplet.dstAttr._2)) {
        (1.0, orgSim, coauthorSim, textSim, yearSim, venueSim)
      } else {
        (0.0, orgSim, coauthorSim, textSim, yearSim, venueSim)
      }
    })
    graphAfterFirstComputation.edges.map(e => (e.attr._1, Seq[Double](e.attr._2, e.attr._3, e.attr._4, e.attr._5, e.attr._6)))
  }

  def disambiguateByAggregate[T <: ClassificationModel[Vector, T]](initGraph: Graph[VertexAttr, EdgeAttr],
                                                                   model: T, numPartitions: Int): Graph[String, Double] = {
    model.asInstanceOf[LogisticRegressionModel].setThreshold(0.9)
    println("first disambiguation...")
    //第一次更新link分数后的边数组
    val graphAfterFirstComputation = initGraph.mapTriplets(triplet => {
      //边的标签为2时表示合作者关系 这里只计算标签为1的边两端节点的相似分数
      val orgSim = cosine(triplet.srcAttr._4, triplet.dstAttr._4)
      val coauthorSim = jaccard(triplet.srcAttr._5, triplet.dstAttr._5)
      //        val yearSim = computeYearSim(triplet.srcAttr._6, triplet.dstAttr._6)
      val yearSim = 0
      val textSim = cosine(triplet.srcAttr._7, triplet.dstAttr._7)
      val venueSim = cosine(triplet.srcAttr._8, triplet.dstAttr._8)
      val features = new DenseVector(Array(orgSim, coauthorSim, textSim, yearSim, venueSim))
      val prediction = model.predict(features)
      (prediction, orgSim, coauthorSim, textSim, yearSim, venueSim)
    })
    //    println("graphAfterFirstComputation",graphAfterFirstComputation.vertices.count())
    println("aggregate coauthors...")
    val vertexRDDAfterFirstAggregation = graphAfterFirstComputation.aggregateMessages[Array[String]](
      triplet => {
        //通过合作者边传递合作者名称信息
        if (triplet.attr._1 == 1.0) {
          val msgToDst = triplet.srcAttr._5
          val msgToSrc = triplet.dstAttr._5
          //发送给目的节点
          triplet.sendToDst(msgToDst)
          //发送给源节点
          triplet.sendToSrc(msgToSrc)
        } else {
          triplet.sendToDst(Array[String]())
          //发送给源节点
          triplet.sendToSrc(Array[String]())
        }
      },
      {
        //合并合作者信息
        (a, b) => a ++ b
      }
    )
    //    println("vertexRDDAfterFirstAggregation",vertexRDDAfterFirstAggregation.count())
    //    vertexRDDAfterFirstAggregation.persist()
    val graphAfterFirstAggregation = Graph(vertexRDDAfterFirstAggregation, graphAfterFirstComputation.edges)
    //    graphAfterFirstAggregation.persist()
    //    graphAfterFirstComputation.unpersist()
    println("second disambiguation...")
    model.asInstanceOf[LogisticRegressionModel].setThreshold(0.8)
    val graphAfterSecondComputation = graphAfterFirstAggregation.mapTriplets(triplet => {
      //忽略合作者关系边和分数为1的link边 边的属性为2时表示合作者关系 只对link边计算
      if (triplet.attr._1 == 0.0) {
        val coSim = computeCoauthorSim(triplet.srcAttr, triplet.dstAttr)
        val features = new DenseVector(Array(triplet.attr._2, coSim, triplet.attr._4, triplet.attr._5, triplet.attr._6))
        val result = model.predict(features)
        (result, triplet.attr._2, coSim, triplet.attr._4, triplet.attr._5, triplet.attr._6)
      } else
        triplet.attr
    })
    Graph(graphAfterFirstComputation.vertices.mapValues(v => v._1), graphAfterSecondComputation.edges.mapValues(e => e.attr._1))
  }

  /**
   * 通过lr模型消歧
   *
   * @param initGraph     初始网络
   * @param numPartitions 分区数
   * @param model         分类模型
   * @tparam T 贝叶斯/逻辑回归/
   * @return
   */
  def disambiguate[T <: ClassificationModel[Vector, T]](initGraph: Graph[VertexAttr, EdgeAttr],
                                                        model: T, numPartitions: Int): Graph[String, Label] = {
    //    println(initGraph.vertices.take(5).foreach(println(_)) println(initGraph.edges.take(5).foreach(println(_))
    //第一次更新link分数后的边数组
    //    println(model.params)
    val graphAfterFirstComputation = initGraph.mapTriplets(triplet => {
      //边的标签为2时表示合作者关系 这里只计算标签为1的边两端节点的相似分数
      val orgSim = cosine(triplet.srcAttr._4, triplet.dstAttr._4)
      val coauthorSim = jaccard(triplet.srcAttr._5, triplet.dstAttr._5)
      //      val yearSim = computeYearSim(triplet.srcAttr._6, triplet.dstAttr._6)
      val yearSim = 0.0
      val textSim = cosine(triplet.srcAttr._7, triplet.dstAttr._7)
      val venueSim = cosine(triplet.srcAttr._8, triplet.dstAttr._8)
      val features = new DenseVector(Array(orgSim, coauthorSim, textSim, yearSim, venueSim))
      val result = model.predict(features)
      result
    })
    val vertices = graphAfterFirstComputation.vertices.map(v => (v._1, v._2._1))
    //    val edges = graphAfterFirstComputation.edges.map(e => Edge(e.srcId, e.dstId, e.attr))
    //    graphAfterFirstComputation.persist()
    val graph = Graph(vertices, graphAfterFirstComputation.edges)
    graph
  }


  /**
   * 保存网络节点和边的rdd到指定路径 仅保留节点id和属性中的作者名字
   * save the vertex rdd and edge rdd of the network to a specify path, only remain vid and author name in attributes
   *
   * @param graph 作者网络 author-network
   * @param path  保存路径 path to save the graph rdd
   */
  def save(graph: Graph[Name, Label], path: String): Unit = {
    val vOut = path + "/out_v"
    val eOut = path + "/out_e"
    FileUtil.deleteDirectory(vOut)
    FileUtil.deleteDirectory(eOut)
    graph.vertices.map(x => (x._1, x._2))
      .saveAsObjectFile(vOut)
    graph.edges.map(x => Edge(x.srcId, x.dstId, x.attr))
      .saveAsObjectFile(eOut)
  }

  /**
   * 保存网络节点和边的rdd到指定路径 仅保留节点id和属性中的作者名字
   *
   * @param graph 作者网络
   * @param path  保存路径
   */
  def saveForTraining[VD, ED](graph: Graph[VD, ED], path: String): Unit = {
    val vOut = path + "/ml_input_v"
    val eOut = path + "/ml_input_e"
    graph.vertices.saveAsObjectFile(vOut)
    graph.edges.saveAsObjectFile(eOut)
  }

  def getEdgesInFullConnectedGraph(ss: SparkSession, df: DataFrame, name: String, path: String): Unit = {
    //saveByAuthor(graph,name,path+name)
    val df3 = df.select("srcId", "dstId", "probability")
    val arr = df3.collect()
    val writer = new PrintWriter(new File(s"$path/$name.txt"))

    val result = arr.map(x => s"${
      x(0).asInstanceOf[Long]
    } ${
      x(1).asInstanceOf[Long]
    } ${
      x(2).asInstanceOf[DenseVector](1)
    }")
    writer.write(result.mkString("\n"))
    writer.close()
  }

  /**
   * 从图中获得一个联通组件中所有节点对应的边,以及节点之间的相似分数
   *
   * @param ss    sparkSession
   * @param graph 图
   * @param df
   * @param name  名字
   * @param path  txt文件保存路径
   */
  def getEdgesInComponents(ss: SparkSession, graph: Graph[String, Double], df: DataFrame, name: String, path: String): Unit = {
    val df2 = df.where(lower(df("name")) === s"${
      name.replace("_", " ")
    }")
    val df3 = df2.select("srcId", "dstId", "probability")
    val pairs = df3.collect().map(x => (x(0).asInstanceOf[Long], x(1).asInstanceOf[Long], x(2).asInstanceOf[DenseVector](1)))
    //pairs.foreach(println(_))
    val subGraph = graph.subgraph(vpred = (id, authorName) => authorName.equalsIgnoreCase(
      name.replace("_", " ")), epred = t => t.attr == 1)
    val componentsRDD = AnalysisUtil.getComponentsRDD(subGraph) //.filter(_.length > 1)
    for (arr <- componentsRDD if arr.length > 1) {
      val filename = s"$path/${
        arr(0)
      }.txt"
      val out = new FileWriter(filename, false)
      val result = pairs.filter(x => arr.contains(x._1))
      result.foreach(x => {
        out.write(s"${
          x._1
        } ${
          x._2
        } ${
          x._3
        }\n")
      })
      //out.write(result.mkString("\n"))
      out.close()
      //println(result.mkString(","))
    }
  }

  /**
   * 从图的边rdd中构造不包含作者名字的训练数据
   *
   * @param ss
   * @return
   */
  def getDataForClustering(ss: SparkSession, edges: EdgeRDD[EdgeAttr], lrModel: LogisticRegressionModel): DataFrame = {
    val row = edges.map(x => {
      val indices = Array(0, 1, 2, 3, 4)
      (x.srcId,
        x.dstId,
        //小于0的余弦相似分数置为0;;过滤小于0的相似分数.
        new SparseVector(size = indices.length, indices = indices,
          values = Array(x.attr._2, x.attr._3, x.attr._4, x.attr._5, x.attr._6)).toSparse,
        x.attr._1
      )
    }
    )
    import ss.implicits._
    val df = row.toDF("srcId", "dstId", "features", "label")
    val result = lrModel.transform(df)
    result.select("srcId", "dstId", "probability")
  }

  /**
   * 构建用于评测的图
   *
   * @param ss
   * @param path
   * @return
   */
  def buildForAnalyzing(ss: SparkSession, path: String): Graph[String, Double] = {
    val vOut = path + "/out_v"
    val eOut = path + "/out_e"
    val vertexRDD = ss.sparkContext.objectFile[(VertexId, String)](vOut, 200)
    val edgeRDD = ss.sparkContext.objectFile[Edge[Double]](eOut, 200)
    val graph = Graph(vertexRDD, edgeRDD)
    graph
  }

  def dumpVidPidJson(vertices: VertexRDD[String], path: String, name: String): Unit = {
    val data = vertices.map(v => (v._1, v._2)).collect()
    val vidPid = new JSONObject()
    data.foreach(x => vidPid.put(x._1.toString, x._2))
    JsonUtil.saveJson(vidPid, s"$path/$name.json")
  }

  def getDataForFeaturePrediction(ss: SparkSession, initGraph: Graph[VertexAttr, EdgeAttr]): DataFrame = {
    val graph = initGraph.mapTriplets(triplet => {
      //边的标签为2时表示合作者关系 这里只计算标签为1的边两端节点的相似分数
      val orgSim = cosine(triplet.srcAttr._4, triplet.dstAttr._4)
      val coauthorSim = jaccard(triplet.srcAttr._5, triplet.dstAttr._5)
      val yearSim = yearDifference(triplet.srcAttr._6, triplet.dstAttr._6)
      val textSim = cosine(triplet.srcAttr._7, triplet.dstAttr._7)
      val venueSim = cosine(triplet.srcAttr._8, triplet.dstAttr._8)
      new SparseVector(5, Array(0, 1, 2, 3, 4), Array(orgSim, coauthorSim, textSim, yearSim, venueSim))
    })
    import ss.implicits._
    val df = graph.edges.map(t => (t.attr.values(0), new SparseVector(
      4, Array(0, 1, 2, 3), Array(t.attr.values(1), t.attr.values(2),
        t.attr.values(3), t.attr.values(4))))).toDF("label", "features")
    df.filter($"label" =!= 0.0)
    //    val data = df.map(x => (x(0), Array(x(1)))).collect()
    //    val writer = new PrintWriter(new File(s"$path/$name.txt"))
    //    writer.write(data.mkString("\n"))
    //    writer.close()
  }

  def dumpProbability(ss: SparkSession, initGraph: Graph[VertexAttr, EdgeAttr],
                      lrModel: LogisticRegressionModel, path: String, name: String, numPartitions: Int): Unit = {
    val graph = initGraph.mapTriplets(triplet => {
      //边的标签为2时表示合作者关系 这里只计算标签为1的边两端节点的相似分数
      val orgSim = cosine(triplet.srcAttr._4, triplet.dstAttr._4)
      val coauthorSim = jaccard(triplet.srcAttr._5, triplet.dstAttr._5)
      //      val yearSim = computeYearSim(triplet.srcAttr._6, triplet.dstAttr._6)
      val yearSim = 0.0
      val textSim = cosine(triplet.srcAttr._7, triplet.dstAttr._7)
      val venueSim = cosine(triplet.srcAttr._8, triplet.dstAttr._8)
      new SparseVector(5, Array(0, 1, 2, 3, 4), Array(orgSim, coauthorSim, textSim, yearSim, venueSim))
    })
    import ss.implicits._
    val df = graph.triplets.map(t => (t.srcAttr._1, t.dstAttr._1, t.attr)).toDF("srcId", "dstId", "features")
    val data = lrModel.transform(df).select("srcId", "dstId", "probability")
      .map(x => s"${x(0)} ${x(1)} ${x(2).asInstanceOf[DenseVector](1)}").collect()
    val writer = new PrintWriter(new File(s"$path/$name.txt"))
    writer.write(data.mkString("\n"))
    writer.close()
  }

  def spectralClustering_v2(ss: SparkSession, path: String, name: String, model: LogisticRegressionModel): Unit = {
    //    val graph = build(ss, path + name)
    //    val graph2 = runForTraining(graph)
    //    //saveForTraining(graph2, path + name)
    //    //val graph2 = buildForTraining(ss, path + name)
    //    val df = Training.getDataForClustering(ss, graph2, model)
    //    //df.show(10)
    //    //df.write.mode("overwrite").parquet(path + name + "/df")
    //    //val df = ss.read.parquet(path + name + "/df")
    //    //    val graph3 = runForDisambiguation(graph, 100, model)
    //    //    save(graph3, path+name)
    //    val graph4 = buildForAnalyzing(ss, path + name)
    //    getEdgesInComponents(ss, graph4, df, name, path + name)
  }

  /**
   * 输出precision recall fscore值
   *
   * @param ss
   */
  def printPRF(ss: SparkSession, venueVectorMap: java.util.HashMap[String, Vector]): Unit = {
    val path = "/user/root/namedis/"
    val name = "all"
    val model = LogisticRegressionModel.load(path + "lr")
    val graph = build(ss, path + name)
    val graph2 = training(ss, graph)
    //val graph = buildForTraining(ss, path + name)
    //    val df = Training.getDataForClustering(ss, graph2, model)
    //    computePRF(ss, df)
  }

  def genEdgeJSON(ss: SparkSession): Unit = {
    val graph = buildForGAE(ss, "/user/root/namedis/test_pubs/input_v", "/user/root/namedis/test_pubs/out_e", 360)
    //    val vidJson: JSONObject = genVertexIdAuthorIdPaperIdJson(graph)
    //    JsonUtil.saveJson(vidJson, "/root/vid_name_aid_pid.json")
    val file = Source.fromURL(this.getClass.getResource(s"/100.txt"))
    val names = file.getLines().toArray
    val edgesJson = dumpEdgesForGAE(graph, names = names)
    JsonUtil.saveJson(edgesJson, "/root/edges_0.3.json")
  }

  /**
   * 返回某个作者的消歧结果,并将独立的机构为空的作者合并为一个类
   *
   * @param graph
   * @return
   */
  def getDisambiguationResultForNullOrgRecord(graph: Graph[(String, Array[String]), Double]): Array[Array[String]] = {
    val vidPidMap = new util.HashMap[Long, String]()
    val vidPid = graph.vertices.collect()
    vidPid.foreach(t => vidPidMap.put(t._1, t._2._1))
    val vidsNullOrg = graph.vertices.filter(v => v._2._2.length == 0).map(x => x._1).collect()
    val subgraph = graph.subgraph(epred = t => t.attr == 1)
    val components = AnalysisUtil.getComponentsRDD2(subgraph).collect()
    val result = ArrayBuffer[Array[String]]()
    val clusterNullOrg = ArrayBuffer[String]()
    for (component <- components) {
      if (component.length == 1 && vidsNullOrg.contains(component(0))) {
        clusterNullOrg.append(vidPidMap.get(component(0)))
      } else {
        val pids = ArrayBuffer[String]()
        for (vid <- component) {
          val pid = vidPidMap.get(vid)
          pids.append(pid)
        }
        result.append(pids.toArray)
      }
    }
    if (clusterNullOrg.nonEmpty) {
      result.append(clusterNullOrg.toArray)
    }
    result.toArray
  }

  /**
   * 返回某个作者的消歧结果
   *
   * @param graph
   * @return
   */
  def getDisambiguationResult(graph: Graph[String, Double]): Array[Array[String]] = {
    val vidPidMap = new util.HashMap[Long, String]()
    val vidPid = graph.vertices.collect()
    vidPid.foreach(t => vidPidMap.put(t._1, t._2))
    val subgraph = graph.subgraph(epred = t => t.attr == 1)
    val components = AnalysisUtil.getComponentsRDD(subgraph).collect()
    val result = ArrayBuffer[Array[String]]()
    for (component <- components) {
      val pids = ArrayBuffer[String]()
      for (vid <- component) {
        val pid = vidPidMap.get(vid)
        pids.append(pid)
      }
      result.append(pids.toArray)
    }
    result.toArray
  }

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      .master("local[*]")
      //.config("spark.executor.memory", "12g")
      .getOrCreate()
    val path = "d:/namedis/hongbin_liang"
    val name = "hongbin_liang"
    //    val namesSource = Source.fromFile(path + name + "/100.txt")
    //    val names = namesSource.getLines().toArray
    val names = Array[String](name)
  }

}
