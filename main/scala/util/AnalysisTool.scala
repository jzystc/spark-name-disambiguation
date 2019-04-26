package util

import java.io.{File, PrintWriter}
import java.util.concurrent.Executors

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import main.AuthorNetwork
import main.AuthorNetwork.{buildML, save}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, PartitionID, VertexId}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

object AnalysisTool {

  private val executor = Executors.newCachedThreadPool

  /**
    * 从指定的路径读取节点rdd文件和边rdd文件构建图
    *
    * @param path 节点和边rdd文件路径
    * @return
    */
  def loadGraph(ss: SparkSession, path: String): Graph[String, Double] = {
    val vOut = path + "/out_v"
    val eOut = path + "/out_e"
    val vertexRDD = ss.sparkContext.objectFile[(VertexId, String)](vOut, 200)
    val edgeRDD = ss.sparkContext.objectFile[Edge[Double]](eOut, 200)
    val graph = Graph(vertexRDD, edgeRDD)
    graph
  }

  def getPairsByEdges(graph: Graph[String, Double], name: String): Set[(Long, Long)] = {
    val resultRDD = graph.triplets.filter(x => x.attr == 1.0 && x.srcAttr.equalsIgnoreCase(name))
      .map(x => (x.srcId, x.dstId))
    val result = resultRDD.collect().toSet
    result
  }

  /**
    * 从图中获得某个名字对应的所有联通块节点id的RDD
    *
    * @param graph 作者网络
    * @param name  作者名字
    *
    */
  def getComponentsRDD(graph: Graph[String, Double], name: String): RDD[Array[Long]] = {
    val edgeRDD = graph.triplets.filter(x => x.attr == 1.0 && x.srcAttr.equalsIgnoreCase(name))
      .map(x => Edge(x.srcId, x.dstId, x.attr))
    val vertexRDD = graph.vertices.filter(x => x._2.equalsIgnoreCase(name))
    val authorGraph = Graph(vertexRDD, edgeRDD)
    val componentsRDD = authorGraph.connectedComponents()
      .vertices.groupBy(_._2).map(
      line => {
        line._2.map(x => x._1).toArray.sorted
      }
    )
    componentsRDD
  }

  def getResultByName(graph: Graph[String, Double], name: String): ObjectNode = {
    val componentsRDD = getComponentsRDD(graph, name)
    val map = mutable.HashMap[String, Array[Byte]]()
    val mapper = new ObjectMapper()
    //构建 ObjectNode
    val author = mapper.createObjectNode()
    val pids = mapper.createObjectNode()
    author.set(name, pids)
    var n = 0
    componentsRDD.foreach(x => {
      val y = x.map(i => i.toByte)
      pids.put(n.toString, y)
      n += 1
    })
    author
  }

  /**
    * 找出某个作者名字对应的节点对
    */
  def getPairsByAuthorName(componentsRDD: RDD[Array[Long]]): Set[String] = {
    //    println("真实作者数:" + AuthorDao.getAuthorsNumByName(name))
    //    println("实验作者数:" + sum.toString)
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

    val pairs = componentsRDD.map(x => combine(x)).reduce((a, b) => a ++ b)
    pairs.map(x => x.toString())
  }

  def analyze(graph: Graph[String, Double], names: Array[String]): List[Array[String]] = {
    var records = List[Array[String]]()
    for (name <- names) {
      println(name)
      records = records :+ analyzeByName(graph, name)
      /*executor.execute(new Runnable() {
        final val n = name
        final val g = graph

        override def run(): Unit = {
          try {
            println(n)
            records = records :+ analyzeByName(g, n)
          } catch {
            case e: Exception => throw e
          }
        }
      })*/
    }
    //executor.shutdown()
    records
  }


  /**
    * 计算精确度
    *
    * @param tp 预测结果中正确的pairwise数量
    * @param fp 预测结果中错误的pairwise数量
    * @return
    */
  def computePrecision(tp: Int, fp: Int): Double = {
    val precision: Double = 1.0 * tp / (tp + fp)
    precision
  }

  /**
    * 计算召回率
    *
    * @param tp 预测结果中正确的pairwise数量
    * @param fn 预测结果中未找到的正确的pairwise数量
    * @return
    */
  def computeRecall(tp: Int, fn: Int): Double = {
    val recall: Double = 1.0 * tp / (tp + fn)
    recall
  }

  /**
    * 计算fscore
    *
    * @param precision 查准率
    * @param recall    召回率
    * @return
    */
  def computeFscore(precision: Double, recall: Double): Double = {
    val fscore = 2 * precision * recall / (precision + recall)
    fscore
  }

  /**
    * 写入实验得到的pairs到文件中
    *
    * @param pairs 实验得到的pairs
    * @param name  作者名字
    */
  def writePairsToFile(pairs: Set[(Long, Long)], name: String): Unit = {
    val filename = name +
      "_a_" + Weight.alpha + "b_" + Weight.beta + "t_" + Weight.threshold + ".txt"
    val writer = new PrintWriter(new File(System.getProperty("user.dir") + "/src/main/resources/exp/" + filename))
    //val writer = new PrintWriter(new File(outdirectory + filename))
    for (x <- pairs) {
      writer.write(x + "\n")
    }
    writer.close()
  }

  /**
    * 读取文件中的pairs
    *
    * @param name 作者名字
    * @param kind 文件类型 true表示读取真实值 exp表示读取实验值
    * @return
    */
  def readPairsFromFile(name: String, kind: String): Set[String] = {
    var filename = ""
    if (kind.equals("exp")) {
      filename = name.replace(' ', '_') +
        "_a_" + Weight.alpha + "b_" + Weight.beta + "t_" + Weight.threshold + ".txt"
    } else if (kind.equals("true")) {
      filename = name.replace(' ', '_') + ".txt"
    }
    val file = Source.fromURL(this.getClass.getClassLoader.getResource("resources/" + kind + "/" + filename))
    //    val file = Source.fromFile(System.getProperty("user.dir") + "/src/main/resources/" + kind + "/" + filename)
    var data = Set[String]()
    for (line <- file.getLines()) {
      data += line.toString
    }
    data
  }

  /**
    * 读取真实pairwise
    *
    * @param name 作者名字
    * @return
    */
  def readTruePairs(name: String): Set[(Long, Long)] = {
    val filename = name + ".txt"
    //val path = new Path("hdfs://localhost/user/csubigdata/namedis/true/" + filename)
    val file = Source.fromURL(this.getClass.getResource("/resources/true/" + filename))
    //val file = Source.fromURL("hdfs://csubigdata:8020/user/csubigdata/namedis/true/" + filename)
    //val file = Source.fromFile(System.getProperty("user.dir") + "/src/main/resources/true/" + filename)
    //    var data = Set[String]()
    //    for (line <- file.getLines()) {
    //      data += line.toString
    //    }
    //path.getFileSystem(new Configuration())
    val data = file.getLines().map(x => {
      val y = x.replace(" ", "")
      (
        y.substring(y.indexOf('(') + 1, y.indexOf(',')).toLong,
        y.substring(y.indexOf(',') + 1, y.indexOf(')')).toLong)
    }).toSet
    data
  }

  /**
    * 根据作者名字分析结果
    *
    * @param name 名字
    *
    */
  def analyzeByName(graph: Graph[String, Double], name: String): Array[String] = {
    val componentsRDD = getComponentsRDD(graph, name.replace("_", " "))
    //val data1 = getPairsByAuthorName(componentsRDD)
    val data1 = getPairsByEdges(graph, name.replace("_", " "))
    //读取实验值
    //    val data1 = readPairsFromFile(name, "exp")
    //读取真实值
    //    val data2 = readPairsFromFile(name, "true")
    val data2 = readTruePairs(name)
    //    data1.foreach(print(_))
    //    data2.foreach(print(_))
    //取交集

    val data3 = data1 & data2
    val tp: Int = data3.size
    val fp: Int = data1.size - tp
    val fn: Int = data2.size - tp
    val precision = computePrecision(tp, fp)
    val recall = computeRecall(tp, fn)
    val fscore = computeFscore(precision, recall)
    println("name:" + name)
    println("precision:" + precision)
    println("recall:" + recall)
    println("f1:" + fscore)
    //val record = Array[String](name, AuthorDao.getAuthorsNumByName(name).toString, componentsRDD.count.toString, precision.toString, recall.toString, fscore.toString)
    val record = Array[String](name, componentsRDD.count.toString, precision.toString, recall.toString, fscore.toString)
    record
  }

  def get2JumpPair(graph: Graph[String, Double]): Unit = {
    type VMap = Map[VertexId, Int] //定义每个节点存放的数据类型，为若干个（节点编号，一个整数）构成的map，当然发送的消息也得遵守这个类型
    /**
      * 节点数据的更新 就是集合的union
      */
    def vprog(vid: VertexId, vdata: VMap, message: VMap) //每轮迭代后都会用此函数来更新节点的数据（利用消息更新本身），vdata为本身数据，message为消息数据
    : Map[VertexId, Int] = addMaps(vdata, message)

    /**
      * 节点数据的更新 就是集合的union
      */
    def sendMsg(e: EdgeTriplet[VMap, _]): Iterator[(VertexId, Map[VertexId, PartitionID])] = {
      //取两个集合的差集  然后将生命值减1
      val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k) - 1) }.toMap
      val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k) - 1) }.toMap
      if (srcMap.isEmpty)
        Iterator.empty
      else
        Iterator((e.srcId, srcMap)) //发送消息的内容
    }

    /**
      * 消息的合并
      */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
      (spmap1.keySet ++ spmap2.keySet).map { //合并两个map，求并集
        k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue)) //对于交集的点的处理，取spmap1和spmap2中最小的值
      }.toMap

    val two = 2 //这里是二跳邻居 所以只需要定义为2即可
    val newG = graph.mapVertices((vid, _) => Map[VertexId, Int](vid -> two)) //每个节点存储的数据由一个Map组成，开始的时候只存储了 （该节点编号，2）这一个键值对
      .pregel(Map[VertexId, Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)
    //pregel参数
    //第一个参数 Map[VertexId, Int]() ，是初始消息，面向所有节点，使用一次vprog来更新节点的值，由于Map[VertexId, Int]()是一个空map类型，所以相当于初始消息什么都没做
    //第二个参数 two，是迭代次数，此时two=2，代表迭代两次（进行两轮的active节点发送消息），第一轮所有节点都是active节点，第二轮收到消息的节点才是active节点。
    //第三个参数 EdgeDirection.Out，是消息发送方向，out代表源节点-》目标节点 这个方向    //pregel 函数参数    //第一个函数 vprog，是用户更新节点数据的程序，此时vprog又调用了addMaps
    //第二个函数 sendMsg，是发送消息的函数，此时用目标节点的map与源节点的map做差，将差集的数据减一；然后同样用源节点的map与目标节点的map做差，同样差集的数据减一
    //第一轮迭代，由于所有节点都只存着自己和2这个键值对，所以对于两个用户之间存在变关系的一对点，都会收到对方的一条消息，内容是（本节点，1）和（对方节点，1）这两个键值对
    //第二轮迭代，收到消息的节点会再一次的沿着边发送消息，此时消息的内容变成了（自己的朋友，0）    //第三个函数 addMaps, 是合并消息，将map合并（相当于求个并集），不过如果有交集（key相同），那么，交集中的key取值（value）为最小的值。

    //过滤得到二跳邻居 就是value=0 的顶点
    val twoJumpFirends = newG.vertices
      .mapValues(_.filter(_._2 == 0).keys) //由于在第二轮迭代，源节点会将自己的邻居（非目标节点）推荐给目标节点——各个邻居就是目标节点的二跳邻居，并将邻居对应的值减为0，
    //twoJumpFirends.collect().foreach(println(_))

    //    twoJumpFirends.filter(x => x._2 != Set()).foreach(println(_)) //把二跳邻居集合非空的（点，{二跳邻居集合}）打印出来
    val result = twoJumpFirends.filter(x => x._2 != Set()).map(x => x._2.map(y => (x._1, y))).reduce(_ ++ _).toSet
    result.foreach(println(_))
  }

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.local.dir", "/tmp/sparktmp,/var/tmp/sparktmp")
      .getOrCreate()

    val name = "test"
    //val file = Source.fromURL(this.getClass.getClassLoader.getResource("true/xu_xu.txt"))
    val path = "/home/csubigdata/namedis/"
    //val graph = loadGraph(ss, path + name)

    def disambiguate(): Unit = {
      //生成初始网络
      val network = buildML(ss, path + name)
      val lrModel = LogisticRegressionModel.load("/home/csubigdata/namedis/lr")
      //val mpcModel = MultilayerPerceptronClassificationModel.load("/home/csubigdata/namedis/mpc")
      val networkAfter = AuthorNetwork.runML(network, 400, lrModel)
      save(networkAfter, path + name)
      val graph = AnalysisTool.loadGraph(ss, path + name)
      val file = Source.fromURI(this.getClass.getResource("100.txt").toURI)
      val names = file.getLines().toArray
      val records = AnalysisTool.analyze(graph, names)
      CSVUtil.write(path + "100.csv", CSVUtil.header2, records)
    }

    disambiguate()
    ss.stop()
    //analyzeByName(graph, name)
    //    val records = analyze(graph, path + "18.txt")
    //    val r=getResultByName(graph,name)
    //    println(r.toString)


  }
}
