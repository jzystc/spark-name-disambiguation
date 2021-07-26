package example

import breeze.numerics.{pow, sqrt}
import info.debatty.java.stringsimilarity._
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.Weight
//import scala.collection.mutable.Set

object ComputeSimExample {
  /*定义一个全局的用于计算作者所属机构的levenstein编辑距离的类对象*/
  final val levenshtein = new Levenshtein()
  /*定义一个向量类型*/
  type Vec = (Double, Double)
  /*定义节点数据类型*/
  type Node = (Long, (String, Vec, Int, String, Set[Link], Set[Link]))
  /*定义节点属性的数据类型*/
  type NodeData = (String, Vec, Int, String, Set[Link], Set[Link])
  /*定义link边的数据类型*/
  type Link = (Long, Long, Double)


  /*判断相似度分数是否大于阈值，大于则返回1，否则返回原值*/
  def compareSim(sim: Double): Double = {
    if (sim >= Weight.threshold)
      1.0
    else
      sim
  }

  /**
    * 计算余弦相似度
    *
    * @param a
    * @param b
    * @return
    */
  def computeCosSim(a: Vec, b: Vec): Double = {
    val sim: Double = (a._1 * b._1 + a._2 * b._2) / (sqrt(pow(a._1, 2) + pow(a._2, 2)) * sqrt(pow(b._1, 2) + pow(b._2, 2)))
    sim
  }

  /**
    * 计算文本相似度
    */
  def computeTextSim(a: Vec, b: Vec): Double = {
    val vecSim = (1 - Weight.beta) * computeCosSim(a, b)
    vecSim
  }

  /**
    * 获取Option中的Set值
    *
    * @param x
    * @return
    */
  def getSetFromOption(x: Option[Set[Link]]): Set[(Long, Long, Double)] = x match {
    case Some(s) => s
    case None => Set[Link]()
  }

  def getPairsByAuthorName(graph: Graph[NodeData, Double], name: String): Unit = {
    val edgeRDD = graph.triplets.filter(x => x.attr == 1 && x.srcAttr._1.equalsIgnoreCase(name))
      .map(x => Edge(x.srcId, x.dstId, x.attr))
    val vertexRDD = graph.vertices.filter(x => x._2._1.equalsIgnoreCase(name))
    val authorGraph = Graph(vertexRDD, edgeRDD)
    //    authorGraph.edges.foreach(println(_))
    //    println("---------------")
    //    authorGraph.vertices.foreach(println(_))
//    val aidArray = authorGraph.connectedComponents().vertices.groupBy(_._2).map(
//      line => {
//        line._2.map(x => x._1).toArray.sorted
//      }
//    )
   authorGraph.connectedComponents().vertices.groupBy(_._2).foreach(println(_))

//    def combine(vIds: Array[Long]): Set[(Long, Long)] = {
//      var result = Set[(Long, Long)]()
//      for (i <- vIds.indices) {
//        for (j: Int <- vIds.length - 1 to i + 1 by -1) {
//          result += ((vIds(i), vIds(j)))
//        }
//      }
//      result
//    }
////
//    val pairs = aidArray.map(x => combine(x)).reduce((a, b) => a ++ b)
//    pairs
  }

  /**
    * 计算合作者关系相似分数
    */
  def computeCoSim(set1: Set[Link], set2: Set[Link]): Double = {
    //两个set中相同的合作者名字个数
    var numSameNames: Int = 0
    val unionSet = set1 & set2
    var coSim = 0.0
    /*遍历交集 加上每条link边乘以系数后的增益分数*/
    val coCoefficient = 0.1
    for (x <- unionSet) {
      coSim += coCoefficient * x._3
    }
    //    numSameNames = unionSet.count(x => x._3 != 0)
    //    val coSim = Weight.coCoefficient * numSameNames
    if (coSim > Weight.alpha)
      Weight.alpha
    else
      coSim
  }

  /**
    * 计算机构相似性
    *
    * @param a
    * @param b
    *
    */
  def computeOrgSim(a: String, b: String): Double = {
    var orgSim = 0.0
    if (a.equals(b)) {
      orgSim = Weight.alpha
    }
    orgSim
  }

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // 创建节点(VertexId,Vec,Int,String)
    val vertexArray = Array(
      (1L, ("A", (1.0, 3.0), 0, "csu", Set[Link](), Set[Link]())),
      (2L, ("A", (1.0, 3.0), 0, "csu", Set[Link](), Set[Link]())),

      (3L, ("A", (1.0, 6.0), 0, "csu", Set[Link](), Set[Link]())),
      (4L, ("B", (1.0, 6.0), 2, "csu", Set[Link](), Set[Link]())),

      (5L, ("A", (2.0, 0.8), 3, "pku", Set[Link](), Set[Link]())),
      (6L, ("C", (2.0, 0.8), 4, "pku", Set[Link](), Set[Link]())),

      (7L, ("A", (1.0, 3.0), 0, "csu", Set[Link](), Set[Link]())),
      (8L, ("D", (1.0, 6.0), 7, "csu", Set[Link](), Set[Link]())),

      (9L, ("A", (1.0, 3.0), 0, "csu", Set[Link](), Set[Link]())),
      (10L, ("D", (1.0, 6.0), 7, "csu", Set[Link](), Set[Link]())))

    /*
    属性值属于[0.0,1.0]时，代表边两端节点的相似分数
    属性值为2.0时，边代表合作者关系
     */
    /* val edgeArray = Array(
      Edge(1L, 2L, 2.0),
      Edge(3L, 4L, 2.0),
      Edge(5L, 6L, 2.0),
      Edge(2L, 4L, 0.0),
      Edge(1L, 3L, 0.0),
      Edge(3L, 5L, 0.0),
      Edge(1L, 7L, 2.0),
      Edge(2L, 7L, 2.0),
      Edge(1L, 7L, 2.0),
      Edge(3L, 8L, 2.0),
      Edge(4L, 8L, 2.0),
      Edge(7L, 8L, 0.0),
      Edge(1L, 5L, 0.0))*/
    val edgeArray = Array(
/*
      Edge(1L, 2L, 2.0),
      Edge(3L, 4L, 2.0),
      Edge(5L, 6L, 2.0),
      Edge(2L, 4L, 1.0),
      Edge(1L, 3L, 1.0),
      Edge(3L, 5L, 1.0),
      Edge(1L, 5L, 0.0),
      Edge(7L, 9L, 1.0),
      Edge(7L, 8L, 2.0),
      Edge(9L, 10L, 2.0))
*/
      Edge(1L, 2L, 1.0),
      Edge(2L, 3L, 1.0)

   )


    // 构建初始节点RDD和边RDD
    val vertexRDD: RDD[Node] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)

    //  构图
    var graph: Graph[NodeData, Double] = Graph(vertexRDD, edgeRDD)
    val pairs: Unit = getPairsByAuthorName(graph, "A")
    //pairs.foreach(println(_))
  }

  /* var graph1 = graph.mapTriplets(x => {
     if (x.attr == 1)
       x
   })
   /*
       //    graph1.edges.foreach(println(_))
       val edgerdd = graph1.edges.filter(_.attr != ())
       //    edgerdd.foreach(println(_))
       //    graph1.vertices.foreach(println(_))
       val graph2 = Graph.fromEdges(edgerdd, graph1.vertices)
       graph2.edges.foreach(println(_))
       //val a=graph2.connectedComponents().vertices.groupBy(_._2)
       //a.foreach(println(_))
       val a = graph2.connectedComponents().vertices.groupBy(_._2).map(
         line => {
           line._2.map(x => x._1).toArray.sorted
         }
       )

       def combine(vIds: Array[Long]): Set[(Long, Long)] = {
         var result = Set[(Long, Long)]()
         for (i: Int <- 0 until vIds.length) {
           for (j: Int <- vIds.length - 1 to i + 1 by -1) {
             result += ((vIds(i), vIds(j)))
           }
         }
         result
       }

       val b = a.map(x => combine(x)).reduce((a, b) => a ++ b)
       println(b)*/

   //    图可视化
   /*    val pw = new PrintWriter("./graph.gexf")
       pw.write(GraphVisualization.toGexf(graph))
       pw.close()*/
   //输出节点信息
   println("[1] 输出初始节点信息")
   graph.vertices.filter { case (id, (name, vec, authorId, org, set1, set2)) => name != "" }.collect.foreach {
     case (id, (name, vec, authorId, org, set1, set2)) => println(s"Name: $name, AuthorId: $authorId, Vec: $vec, Org: $org")
   }

   /**
     * 关键代码，传递消息，计算相似度分数
     */


   println("[2] 第1次计算link的相似分数")
   //第一次更新link相似分数 authorid相同的节点相似分数设为1
   val edgeArray2 = graph.mapTriplets(triplet => { //第一次更新link分数后的边数组
     //边的属性为2时表示合作者关系 只对link边计算
     if (triplet.attr != 2 && triplet.attr != 1) {
       //authorid相同且不等于0(0表示缺省)则返回1(1表示合并节点),否则返回余弦相似度
       //id为int类型....数据库中为字符串
       if ((triplet.srcAttr._3 != triplet.dstAttr._3) || (triplet.srcAttr._3 == 0) || (triplet.dstAttr._3 == 0)) {
         val textSim = computeTextSim(triplet.srcAttr._2, triplet.dstAttr._2)
         val orgSim = computeOrgSim(triplet.srcAttr._4, triplet.dstAttr._4)
         val coSim = computeCoSim(triplet.srcAttr._6, triplet.dstAttr._6)
         val sim = textSim + orgSim + coSim
         // 相似度分数大于阈值则合并节点
         compareSim(sim)
       } else {
         1.0
       }
     } else
       triplet.attr
   }).triplets.collect.map(x => Edge(x.srcId, x.dstId, x.attr))
   //    edgeArray2.foreach(println(_))
   //新的边rdd
   val edgeRDD2: RDD[Edge[Double]] = sc.parallelize(edgeArray2)

   // 构造新图
   val graph2: Graph[NodeData, Double] = Graph(vertexRDD, edgeRDD2)
   graph = graph2

   var check = true
   while (check) {
     println("[3] 将每个节点的link分数添加到节点中")
     // 把与当前节点相连的link分数传给当前节点
     val vertexRDD2 = graph.aggregateMessages[Set[Link]](
       // edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
       //triplet=>{triplet.sendToDst((triplet.srcAttr.name, triplet.dstAttr.age - triplet.srcAttr.age))},
       triplet => {
         //2表示合作者关系,此处只对link边进行操作
         if (triplet.attr != 2.0) {
           val set1 = Set[Link]((triplet.srcId, triplet.dstId, triplet.attr))
           //发送给目的节点
           triplet.sendToDst(set1)
           //发送给源节点
           triplet.sendToSrc(set1)
         }
       },
       {
         //合并link边发来的分数集合
         (a, b) => b ++ a
       }
     )
     graph.edges.foreach(println(_))

     //合并vertexRDD2与graph2的节点RDD
     val vertexRDD3 = graph.vertices.leftOuterJoin(vertexRDD2)
       /*
       x._1: 节点id
       x._2._1._1: 作者名
       x._2._1._2: 向量
       x._2._1._3: authorId
       getSetFromOption(x._2._2): 与当前节点相连的所有link边的信息List[(id1,id2,sim)]
        */
       .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, getSetFromOption(x._2._2), x._2._1._6)))
     //      vertexRDD3.foreach(println(_))
     var graph3 = Graph(vertexRDD3, graph.edges)
     //  graph3.vertices.foreach(println(_))
     println("[4] 将当前节点邻居节点的link分数集合添加到节点中")
     val vertexRDD4 = graph3.aggregateMessages[Set[Link]](
       // For each edge send a message to the destination vertex with the attribute of the source vertex
       // edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
       //triplet=>{triplet.sendToDst((triplet.srcAttr.name, triplet.dstAttr.age - triplet.srcAttr.age))},
       triplet => {
         //2表示合作者关系
         if (triplet.attr == 2.0) {
           //author_id相同，边的属性设为1
           triplet.sendToDst(triplet.srcAttr._5)
           triplet.sendToSrc(triplet.dstAttr._5)
         }
       },
       {
         (a, b) => a ++ b
       }
     )
     //  vertexRDD4.foreach(println(_))
     val vertexRDD5 = graph3.vertices.leftOuterJoin(vertexRDD4)
       .map(x =>
         (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, getSetFromOption(x._2._2))))
     //      vertexRDD5.foreach(println(_))
     var graph4 = Graph(vertexRDD5, graph3.edges)

     //      graph4.edges.foreach(println(_))

     //    graph = graph4   //保留原有结


     println("[5] 计算合作者增益并更新当前节点对应的link分数")
     //计算合作者增益并更新link分数
     val edgeArray3 = graph4.mapTriplets(triplet => {
       //忽略合作者关系边和分数为1的link边，边的属性为2时表示合作者关系 只对link边计算
       if (triplet.attr != 2 && triplet.attr != 1) {
         val coSim = computeCoSim(triplet.srcAttr._5, triplet.dstAttr._5)
         val textSim = computeTextSim(triplet.srcAttr._2, triplet.dstAttr._2)
         val orgSim = computeOrgSim(triplet.srcAttr._4, triplet.dstAttr._4)
         val sim = textSim + orgSim + coSim
         compareSim(sim)
       } else
         triplet.attr
     }).triplets.collect.map(x => Edge(x.srcId, x.dstId, x.attr))
     val edgeRDD3 = sc.makeRDD(edgeArray3)
     //      edgeRDD3.foreach(println(_))
     println("[6] 输出合并节点")
     val graph5 = Graph(vertexRDD5, edgeRDD3)
     graph5.vertices.foreach(println(_))
     //      val check:Boolean =

     //      println("-------before-------")
     val before = graph.edges.filter(_.attr == 1).collect()
     //      println(before.foreach(println(_)))

     //      println("--------now---------")
     val after = graph5.edges.filter(_.attr == 1).collect()
     //      println(after.foreach(println(_)))


     check = !(before sameElements after)
     //      println(check)


     graph = graph5
     //      graph.edges.foreach(println(_))
     graph.edges.foreach(
       x => {
         if (x.attr == 1.0)
           println(x)
       })
   }
   var rs = graph.triplets
     .collect
     .map(et =>
       if (et.attr == 1.0) {
         if (et.srcAttr._1.equals("A")) {
           (et.srcId, et.dstId)
         }
       }).toSet - ()
   //      if (et.attr == 1.0) {
   //        if (et.srcAttr._1.equals("A")) {
   ////          rs = rs.+((et.srcId, et.dstId))
   //
   //        }


   rs.foreach(println(_))
   println("------Count------")
   println(rs.count(x => x != ()))

 }

 def levensteinRatio(s1: String, s2: String): Double = {
   val sum = s1.size + s2.size
   val levenDist = levenshtein.distance(s1, s2)
   val levenRatio = (sum - levenDist) / sum
   levenRatio
 }

 def jaroWrinkler(s1: String, s2: String): Double = {
   val jaroWinkler = new JaroWinkler()
   val jaroDist = jaroWinkler.distance(s1, s2)
   1 - jaroDist
 }*/
}
