package util

import java.io.{File, FileWriter}

import com.alibaba.fastjson.JSONObject
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object AnalysisUtil {


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

  /**
   * 根据一跳邻居找节点对
   *
   * @param graph
   * @return
   */
  def get1JumpPair(graph: Graph[String, Double]): Set[(Long, Long)] = {
    val resultRDD = graph.triplets.filter(x => x.attr == 1.0)
      .map(x => (x.srcId, x.dstId))
    val result = resultRDD.collect().toSet
    result
  }

  /**
   * 从图中获得某个名字对应的所有联通块节点id的RDD
   *
   * @param graph 作者网络
   * @return [连通块i的vid数组]
   */
  def getComponentsRDD[VD, ED](graph: Graph[String, Double]): RDD[Array[Long]] = {
    val connectedComponents = graph.connectedComponents().vertices.groupBy(_._2).map(
      row => {
        row._2.map(x => x._1).toArray
      }
    )
    connectedComponents
  }

  def getComponentsRDD2[VD, ED](graph: Graph[(String, Array[String]), Double]): RDD[Array[Long]] = {
    val connectedComponents = graph.connectedComponents().vertices.groupBy(_._2).map(
      row => {
        row._2.map(x => x._1).toArray
      }
    )
    connectedComponents
  }

  /**
   * 获取连通块中的边
   *
   * @param path 连通块边保存路径
   */
  def getAdjacentMatrixTxt(graph: Graph[String, Double], name: String, path: String): Int = {
    val subGraph = graph.subgraph(vpred = (id, author) => author.equalsIgnoreCase(name.replace("_", " ")), epred = t => t.attr == 1)
    val componentsRDD = getComponentsRDD(subGraph) //.filter(_.length > 1)
    val num = componentsRDD.count()
    println(s"$name:$num")
    val pairs = subGraph.edges.map(e => {
      (e.srcId, e.dstId)
    }).collect.toSet

    for (arr <- componentsRDD) {
      val filename = s"$path/${arr(0)}.txt"
      val out = new FileWriter(filename, false)
      val result = pairs.filter(x => arr.contains(x._1))
      result.foreach(x => {
        out.write(s"${x._1} ${x._2}\n")
      })
      //out.write(result.mkString("\n"))
      out.close()
      //println(result.mkString(","))
    }
    num.toInt
  }


  def getResultByName(graph: Graph[String, Double], name: String): ObjectNode = {
    val componentsRDD = getComponentsRDD(graph)
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
   * 两两组合名字相同节点对应的vIds
   *
   * @param vIds vertexId 数组
   * @return
   */
  def combine(vIds: Array[Long]): Set[(Long, Long)] = {
    val result = mutable.Set[(Long, Long)]()
    for (i <- vIds.indices) {
      for (j <- vIds.length - 1 to i + 1 by -1) {
        result.add(if (vIds(i) > vIds(j)) (vIds(j), vIds(i)) else (vIds(i), vIds(j)))
      }
    }
    result.toSet
  }

  /**
   * 找出某个作者名字对应的节点对
   */
  def getPairsByAuthorName(componentsRDD: RDD[Array[Long]]): Set[(Long, Long)] = {
    //    println("真实作者数:" + AuthorDao.getAuthorsNumByName(name))
    //    println("实验作者数:" + sum.toString)
    try {
      val pairs = componentsRDD.map(x => combine(x)).reduce((a, b) => a ++ b)
      //pairs.map(x => x.toString())
      pairs
    }
    catch {
      case exception: Exception =>
        println("empty collection")
        val pairs = Set[(Long, Long)]()
        pairs
    }
  }

  /**
   * 评测多个名字的结果
   *
   * @param graph 用于评测的作者-文献网络
   * @param names 作者名列表
   * @param pairs 评测参照集rdd
   * @return
   */
  def analyze(graph: Graph[String, Double], names: Array[String], pairs: RDD[(VertexId, VertexId, String)]): List[Array[String]] = {
    var records = List[Array[String]]()
    for (name <- names) {
      //println(name)
      val truePairs = getTruePairs(pairs, name)
      records = records :+ analyzeByName(graph, name, truePairs, 0)
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
   * 评测多个名字的结果
   *
   * @param graph          用于评测的作者-文献网络
   * @param names          作者名列表
   * @param evaluationJson 评测参照集json
   * @return 评测指标记录数组
   */
  def analyzeByJson(graph: Graph[String, Double], names: Array[String], evaluationJson: JSONObject): List[Array[String]] = {
    val records = ListBuffer[Array[String]]()
    for (name <- names) {
      val (realNum, truePairs) = getTruePairsFromJson(evaluationJson, name)
      records.append(analyzeByName(graph, name, truePairs, realNum))
    }
    records.toList
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

  //  /**
  //    * 写入实验得到的pairs到文件中
  //    *
  //    * @param pairs 实验得到的pairs
  //    * @param name  作者名字
  //    */
  //  def writePairsToFile(pairs: Set[(Long, Long)], name: String): Unit = {
  //    val filename = name +
  //      "_a_" + Weight.alpha + "b_" + Weight.beta + "t_" + Weight.threshold + ".txt"
  //    val writer = new PrintWriter(new File(System.getProperty("user.dir") + "/src/main/resources/exp/" + filename))
  //    //val writer = new PrintWriter(new File(outdirectory + filename))
  //    for (x <- pairs) {
  //      writer.write(x + "\n")
  //    }
  //    writer.close()
  //  }

  //  /**
  //    * 读取文件中的pairs
  //    *
  //    * @param name 作者名字
  //    * @param kind 文件类型 true表示读取真实值 exp表示读取实验值
  //    * @return
  //    */
  //  def readPairsFromFile(name: String, kind: String): Set[String] = {
  //    var filename = ""
  //    if (kind.equals("exp")) {
  //      filename = name.replace(' ', '_') +
  //        "_a_" + Weight.alpha + "b_" + Weight.beta + "t_" + Weight.threshold + ".txt"
  //    } else if (kind.equals("true")) {
  //      filename = name.replace(' ', '_') + ".txt"
  //    }
  //    val file = Source.fromURL(this.getClass.getClassLoader.getResource("resources/" + kind + "/" + filename))
  //    //    val file = Source.fromFile(System.getProperty("user.dir") + "/src/main/resources/" + kind + "/" + filename)
  //    var data = Set[String]()
  //    for (line <- file.getLines()) {
  //      data += line.toString
  //    }
  //    data
  //  }

  /**
   * 读取真实pairwise
   *
   * @param name 作者名字
   * @return
   */
  def readTruePairs(name: String): Set[(Long, Long)] = {
    val filename = name + ".txt"
    //val path = new Path("hdfs://localhost/user/csubigdata/namedis/true/" + filename)
    //println(this.getClass.getResource("/resources/true/" + filename))
    val file = Source.fromURL(this.getClass.getResource("/true/" + filename))
    //val file = Source.fromFile("/home/csubigdata/namedis/true/" + filename)
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
  def analyzeByName(graph: Graph[String, Double], name: String, truePairs: Set[(VertexId, VertexId)], realNum: Int): Array[String] = {
    val subGraph = graph.subgraph(epred = t => t.attr == 1.0,
      vpred = (id, str) => str.equals(name))
    subGraph.vertices.take(3)
    subGraph.edges.take(3)
    val componentsRDD = getComponentsRDD(subGraph)
    val expPairs = getPairsByAuthorName(componentsRDD)
    //val data1 = getExpPairs(subGraph)
    //取交集
    val overlap = expPairs & truePairs
    val tp: Int = overlap.size
    val fp: Int = expPairs.size - tp
    val fn: Int = truePairs.size - tp
    val precision = computePrecision(tp, fp)
    val recall = computeRecall(tp, fn)
    val fscore = computeFscore(precision, recall)
    val expNum = componentsRDD.count.toString
    println(s"name: $name")
    println(s"real num: $realNum")
    println(s"exp num: $expNum")
    println(s"precision: $precision")
    println(s"recall: $recall")
    println(s"f1: $fscore")
    //val record = Array[String](name, AuthorDao.getAuthorsNumByName(name).toString, componentsRDD.count.toString, precision.toString, recall.toString, fscore.toString)
    val record = Array[String](name, realNum.toString, expNum, precision.toString, recall.toString, fscore.toString)
    record
  }

  /**
   * 从评测参照集rdd中获取指定名字的集合
   *
   * @param truepairs 评测参照集rdd
   * @param name      作者名
   * @return
   */
  def getTruePairs(truepairs: RDD[(VertexId, VertexId, String)], name: String): Set[(Long, Long)] = {
    val pairs = truepairs.filter(_._3.equalsIgnoreCase(name.replace("_", " "))).map(x => (x._1, x._2)).collect().toSet
    pairs
  }

  def getTruePairsFromJson(jsonObj: JSONObject, name: String): (Int, Set[(Long, Long)]) = {
    val vertexIdArr = jsonObj.get(name).asInstanceOf[Array[Array[Long]]]
    var pairs = Set[(Long, Long)]()
    for (arr <- vertexIdArr) {
      if (arr.length > 1) {
        pairs ++= (combine(arr))
      }
      //      arr.foreach(print(_))
    }
    (vertexIdArr.length, pairs)
  }

  /**
   * 从最后生成的单个作者的连通子图中作者-文献网络中获取实验得到的pairs
   *
   * @param graph 单个作者的连通子图
   * @return
   */
  def getExpPairs(graph: Graph[String, Double]): Set[(Long, Long)] = {

    //    val edges = graph.triplets.filter(x => x.attr == 1.0 && x.srcAttr.equalsIgnoreCase(name))
    //      .map(x => Edge(x.srcId, x.dstId, x.attr))
    //    val subGraph = Graph.fromEdges(edges, name)
    //    val authorGraph = Graph.fromEdges(edges, String)
    try {
      val result = get2JumpPair(graph) ++ get1JumpPair(graph)
      result
    }
    catch {
      case ex: java.lang.UnsupportedOperationException =>
        println("ex: java.lang.UnsupportedOperationException")
        val result = get1JumpPair(graph)
        result
    }
  }

  def get2JumpPair(graph: Graph[String, Double]): Set[(Long, Long)] = {
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
    result
  }

  /**
   * 对谱聚类的结果进行评测
   *
   * @param name      作者名
   * @param expPairs  实验pairs
   * @param truePairs 真实pairs
   * @param num       作者分类数
   * @return
   */
  def analyzeByClustering(name: String, expPairs: Set[(Long, Long)], truePairs: Set[(Long, Long)], num: Int): Array[String] = {
    val intersection = expPairs & truePairs
    val tp: Int = intersection.size
    val fp: Int = expPairs.size - tp
    val fn: Int = truePairs.size - tp
    val precision = computePrecision(tp, fp)
    val recall = computeRecall(tp, fn)
    val fscore = computeFscore(precision, recall)
    println("name:" + name)
    //val expNum = components.count.toString
    //println(s"exp num:" + expNum)
    println("precision:" + precision)
    println("recall:" + recall)
    println("f1:" + fscore)
    val record = Array(name, num.toString, precision.toString, recall.toString, fscore.toString)
    record
  }

  def getPairsFromSource(file: Source): (Set[(Long, Long)], Int) = {
    val data = file.mkString
    val clusters = data.split('\n')
    val num = clusters.length
    println(s"clusters num:$num")
    var pairs = Set[(Long, Long)]()
    for (cluster <- clusters) {
      val vidstr = cluster.trim.split('\t')
      val vids = vidstr.map(_.trim.toLong)
      val set = combine(vids)
      //println(set.mkString(","))
      pairs = pairs ++ set
    }
    println(s"pairs num:${pairs.count(x => x._1 != 0)}")
    (pairs, num)
  }

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      //.config("spark.local.dir", "/tmp/sparktmp,/var/tmp/sparktmp")
      .getOrCreate()
    val path = "d:/namedis/"
    //val name = "xu_xu"


    def run(namelist: String): Unit = {
      val graph = loadGraph(ss, path + "all")
      val file = Source.fromURL(this.getClass.getResource(s"/resources/$namelist"))
      val names = file.getLines()
      for (name <- names) {
        val dir = new File(s"$path/all/components/$name")
        if (!dir.exists()) {
          dir.mkdir()
        }
        getAdjacentMatrixTxt(graph, name, s"$path/all/components/$name")
      }
    }

    def evaluate(nameList: String, filename: String): Unit = {
      val graph = loadGraph(ss, path + "all")
      val rdd = ss.sparkContext.objectFile[(Long, Long, String)](s"$path/truepairs")
      rdd.cache()
      val file = Source.fromURL(this.getClass.getResource(s"/resources/$nameList"))
      val names = file.getLines()
      var records = List[Array[String]]()
      for (name <- names) {
        try {
          val subGraph = graph.subgraph(vpred = (id, author) => author.equalsIgnoreCase(name.replace("_", " ")), epred = t => t.attr == 1)
          val componentsRDD = getComponentsRDD(subGraph).filter(_.length == 1)
          val soleNum = componentsRDD.count().toInt
          val source = Source.fromURL(this.getClass.getResource(s"/resources/result/clustering_result_$name.txt"))
          val (expPairs, clusterNum) = getPairsFromSource(source)
          //val truePairs = readTruePairs(name)
          val truePairs = getTruePairs(rdd, name)
          records = records :+ analyzeByClustering(name, expPairs, truePairs, soleNum + clusterNum)
        } catch {
          case ex: java.lang.NullPointerException => ()
        }
      }
      CSVUtil.write(s"$path/spectralclustering_$filename.csv", CSVUtil.HEADER_EXP_PRF, records)
    }

    def evaluateByName(name: String, filename: String): Unit = {
      //val subGraph = graph.subgraph(vpred = (id, author) => author.equalsIgnoreCase(name.replace("_", " ")), epred = t => t.attr == 1)
      //val componentsRDD = getComponentsRDD(subGraph).filter(_.length == 1)
      // val soleNum = componentsRDD.count().toInt
      val location = this.getClass.getResource(s"/result/clustering_result_$name.txt")
      println(location)
      val source = Source.fromURL(location)
      val (expPairs, clusterNum) = getPairsFromSource(source)
      val truePairs = readTruePairs(name)
      //        val rdd = ss.sparkContext.objectFile[(Long, Long, Name)](s"$path/truepairs")
      //        rdd.cache()
      //val truePairs = getTruePairs(rdd, name)
      // println(analyzeByClustering(name, expPairs, truePairs, soleNum + clusterNum).mkString(","))
      val record = analyzeByClustering(name, expPairs, truePairs, clusterNum)
    }

    //run("89.txt")
    //evaluate("test.txt", "test")
    val name = "xu_xu"
    evaluateByName(name, name)
    //println(this.getClass.getResource(s"/result/clustering_result_$name.txt"))
    ss.stop()
  }
}
