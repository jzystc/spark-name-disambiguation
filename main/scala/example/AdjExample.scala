package example

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AdjExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    //    val graph: Graph[Double, Int] =
    //      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapVertices( (id, _) => id.toDouble )
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
      Array(
        (0L, ("rxin", "student")),
        (1L, ("jgonzal", "postdoc")),
        (2L, ("franklin", "prof")),
        (3L, ("istoica", "prof")),
        (4L, ("messi", "prof")),
        (5L, ("cr7", "prof")),
        (5L, ("hala", "prof"))
      )
    )
    val relationships: RDD[Edge[String]] = sc.parallelize(
      Array(
        Edge(0L, 1L, "collab"),
        Edge(0L, 2L, "advisor"),
        Edge(1L, 2L, "colleague"),
        Edge(2L, 3L, "pi"),
        Edge(3L, 4L, "pi"),
        Edge(4L, 5L, "pi"),
        Edge(4L, 6L, "pi"),
        Edge(5L, 6L, "pi")
      )
    )
    val graph = Graph(users, relationships)
    graph.edges.collect().foreach(println(_))
    //    graph.vertices.map(node => Pregel(
    //      graph.mapVertices((vid, value) => if (vid == node._1) 2 else -1), //初始化信息，源节点为2，其他节点为-1
    //      -1,
    //      2,
    //      EdgeDirection.Out
    //    )(
    //      vprog = (vid, attr, msg) => math.max(attr, msg), //顶点操作，到来的属性和原属性比较，较大的作为该节点的属性
    //      edge => {
    //        if (edge.srcAttr <= 0) {
    //          if (edge.dstAttr <= 0) {
    //            Iterator.empty //都小于0，说明从源节点还没有传递到这里
    //          } else {
    //            Iterator((edge.srcId, edge.dstAttr - 1)) //目的节点大于0，将目的节点属性减一赋值给源节点
    //          }
    //        } else {
    //          if (edge.dstAttr <= 0) {
    //            Iterator((edge.dstId, edge.srcAttr - 1)) //源节点大于0，将源节点属性减一赋值给目的节点
    //          } else {
    //            Iterator.empty //都大于0，说明在二跳节点以内，不操作
    //          }
    //        }
    //      },
    //      (a, b) => math.max(a, b) //当有多个属性传递到一个节点，取大的，因为大的离源节点更近
    //    ).vertices.filter(_._2 != 0).map(v => (node._1, v._1))
    //    ).collect().foreach(println(_));
    //pregel实现
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
}
