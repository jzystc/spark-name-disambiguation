package main

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.sql.SparkSession
import util.Similarity._
import util.Weight

import scala.reflect.ClassTag

object AuthorNetwork {
  /**
    * 定义link边传递的消息的数据类型
    * 1 srcId
    * 2 dstId
    * 3 sim score
    */
  type LinkMsg = (Long, Long, Double)

  /**
    * 定义边属性的数据类型
    * 1. 2.0:合作者关系,1.0:link边两端节点已消歧,0-1:link边两端节点的相似度
    * 2 link边两端节点的文本和机构相似分数
    * 3 link边两端节点的层次(发表年份)缩放系数
    **/
  type EdgeAttr = (Double, Double, Double)

  /**
    * 定义用于训练参数的边属性的数据类型
    * 1.lable(0: id相同 1: id不同)
    * 2.机构相似度
    * 3.合作者相似度
    * 4.标题相似度
    * 5.摘要相似度
    **/
  type EdgeML = (Double, Double, Double, Double, Double)
  //  type EdgeML = (Double, Double, Double, Double)
  /**
    * 定义节点属性的数据类型
    * 1.名字
    * 2.作者id
    * 3.机构向量
    * 4.发表年份
    * 5.标题向量val v1 = new DenseVector[Double](v1)
    * 6.摘要向量
    */
  type VertexAttr = (String, String, Vector, Int, Vector, Vector)

  //type VertexAttr = (String, String, Vector, String, Int, Vector)
  def buildML(ss: SparkSession, path: String): Graph[VertexAttr, EdgeML] = {
    val vIn = path + "/ml_input_v"
    val eIn = path + "/ml_input_e"
    val vertexDF = ss.read.load(vIn)
    val edgeDF = ss.read.load(eIn)
    //创建临时视图
    vertexDF.createOrReplaceTempView("vertex")
    edgeDF.createOrReplaceTempView("edge")
    //从临时视图中获取数据
    val vertexSQL = ss.sql("select id,name,author_id,orgVec,year,titleVec,abstractVec from vertex")
    val edgeSQL = ss.sql("select srcId,dstId,label,orgSim,coauthorSim,titleSim,abstractSim from edge")
    //构建节点RDD
    val vertexRDD = vertexSQL.rdd.map(x =>
      (
        //vertexId
        x(0) match {
          case i: Int => i.asInstanceOf[Int].toLong
          case j: Long => j.asInstanceOf[Long]
        },
        (
          //名字
          x(1).asInstanceOf[String],
          //作者id
          {
            if (x(2) != null)
              x(2).asInstanceOf[String]
            else
              ""
          },
          //机构
          //          {
          //            if (x(3) != null)
          //              x(3).asInstanceOf[String]
          //            else
          //              ""
          //          },

          x(3).asInstanceOf[Vector],
          //发表年份
          x(4).asInstanceOf[Int],
          //标题向量
          x(5).asInstanceOf[Vector],
          //摘要向量
          x(6).asInstanceOf[Vector]
        )
      )
    )
    //构建边RDD
    val edgeRDD = edgeSQL.rdd.map(x =>
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
          //titleSim
          x(5).asInstanceOf[Double],
          //abstractSim
          x(6).asInstanceOf[Double]
        )
      )
    )
    val network = Graph(vertexRDD, edgeRDD)
    network
  }

  /**
    * 读取指定位置下的节点和边dataframe文件来构建作者网络
    *
    * @param path 节点和边dataframe文件所在路径
    * @return
    */
  def build(ss: SparkSession, path: String): Graph[VertexAttr, EdgeAttr] = {
    val vIn = path + "/input_v"
    val eIn = path + "/input_e"
    val vertexDF = ss.read.load(vIn)
    val edgeDF = ss.read.load(eIn)
    //创建临时视图
    vertexDF.createOrReplaceTempView("vertex")
    edgeDF.createOrReplaceTempView("edge")
    //从临时视图中获取数据
    val vertexSQL = ss.sql("select id,name,author_id,orgVec,year,titleVec,abstractVec from vertex")
    val edgeSQL = ss.sql("select srcId,dstId,label,oldLabel,orgSim from edge")
    //构建节点RDD
    val vertexRDD = vertexSQL.rdd.map(x =>
      (
        //vertexId
        x(0) match {
          case i: Int => i.asInstanceOf[Int].toLong
          case j: Long => j.asInstanceOf[Long]
        },
        (
          //名字
          x(1).asInstanceOf[String],
          //作者id
          {
            if (x(2) != null)
              x(2).asInstanceOf[String]
            else
              ""
          },
          //机构
          //          {
          //            if (x(3) != null)
          //              x(3).asInstanceOf[String]
          //            else
          //              ""
          //          },
          x(3).asInstanceOf[Vector],
          //文章发表年份
          x(4).asInstanceOf[Int],
          //标题向量
          x(5).asInstanceOf[Vector],
          //摘要向量
          x(6).asInstanceOf[Vector]
        )
      )
    )
    //构建边RDD
    val edgeRDD = edgeSQL.rdd.map(x =>
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
        //        x(1).asInstanceOf[Long].toLong,
        //attr,类型为Tuple(Double,Double,Double)
        (x(2).asInstanceOf[Double], x(3).asInstanceOf[Double], x(4).asInstanceOf[Double])
      )
    )
    val network = Graph(vertexRDD, edgeRDD)
    network
  }

  /**
    * 获取Option中的Set数据
    *
    * @param x 待处理的Option数据
    * @return
    */
  private def getSetFromOption(x: Option[Set[LinkMsg]]) = x match {
    case Some(s) => s
    case None => Set[LinkMsg]()
  }

  /**
    * 对一个作者网络中的节点去歧
    *
    * @param g            作者网络
    * @param tolerance    进行下一次迭代的条件是当前每条link边的相似分数变化值大于tolerance
    * @param partitionNum rdd分区数 影响并行计算的速度
    *
    */
  def runUntilConvergence(g: Graph[VertexAttr, EdgeAttr], tolerance: Double, partitionNum: Int): Graph[VertexAttr, EdgeAttr] = {
    g.cache()
    println("[初始化link相似分数]")
    //第一次更新link分数后的边数组
    var graph = g.mapTriplets(triplet => {
      //边的属性为2时表示合作者关系 只对link边计算
      if (triplet.attr._1 != 2.0) {
        //author_id相同且不等于0(0表示缺省)则返回1(1表示合并节点),否则返回文本相似度
        //        if (!triplet.srcAttr._2.equals("")
        //          && !triplet.dstAttr._2.equals("")
        //          && triplet.srcAttr._2.equals(triplet.dstAttr._2)) {
        //          //authorId相同对应边的属性值
        //          (1.0, 0.0, 0.0)
        //        } else {
        //          val orgSim = computeOrgSim(triplet.srcAttr._3, triplet.dstAttr._3)
        //          val textSim = computeVectorSim(triplet.srcAttr._5, triplet.dstAttr._5)
        //          val layerSim = computeLayerSim(triplet.srcAttr._4, triplet.dstAttr._4)
        //          val sim = layerSim * Weight.alpha * ((1 - Weight.beta) * textSim + Weight.beta * orgSim)
        //          // var sim = Weight.alpha * ((1 - Weight.beta) * textSim + Weight.beta * orgSim)
        //          (0.0, 0.0, sim)
        //
        //        }
        val orgSim = computeVectorSim(triplet.srcAttr._3, triplet.dstAttr._3)
        val layerSim = computeLayerSim(triplet.srcAttr._4, triplet.dstAttr._4)
        val titleSim = computeVectorSim(triplet.srcAttr._5, triplet.dstAttr._5)
        val abstractSim = computeVectorSim(triplet.srcAttr._6, triplet.dstAttr._6)

        val sim = layerSim * Weight.alpha * ((1 - Weight.beta) * titleSim + Weight.beta * orgSim)
        // var sim = Weight.alpha * ((1 - Weight.beta) * textSim + Weight.beta * orgSim)
        (0.0, 0.0, sim)
      } else
      //合作者关系对应边的属性值
        (2.0, 0.0, 0.0)
    })

    var check = true
    var iter = 1
    while (check) {
      println("[1] 将每个节点的link分数添加到节点中")
      // 把与当前节点相连的link分数传给当前节点
      var msgRDD = graph.aggregateMessages[Set[LinkMsg]](
        triplet => {
          //2表示合作者关系,此处只对link边进行操作
          if (triplet.attr._1 != 2.0) {
            val msg = Set[LinkMsg]((triplet.srcId, triplet.dstId, triplet.attr._1))
            //发送给目的节点
            triplet.sendToDst(msg)
            //发送给源节点
            triplet.sendToSrc(msg)
          }
        },
        {
          //合并link边发来的分数集合
          (a, b) => a ++ b
        }
      )

      var vertexRDD = graph.vertices
        //        .partitionBy(new HashPartitioner(partitionNum))
        //        .repartition(partitionNum)
        .leftOuterJoin(msgRDD)

        /**
          *  x._1: 节点id
          *  x._2._1._1: 作者名
          *  x._2._1._2: 文本向量
          *  x._2._1._3: author_id
          *  x._2._1._4: org
          *  x._2._1._5: year
          * *
          * getSetFromOption(x._2._2): 与当前节点相连的所有link边的信息List[(id1,id2,sim)]
          */
        .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, getSetFromOption(x._2._2))))
      var graphWithMsg = Graph(vertexRDD, graph.edges)

      println("[2] 将邻居节点的link分数集合添加到当前节点中")
      msgRDD = graphWithMsg.aggregateMessages[Set[LinkMsg]](
        triplet => {
          //2表示合作者关系
          if (triplet.attr._1 == 2.0) {
            //author_id相同，边的属性设为1
            triplet.sendToDst(triplet.srcAttr._6)
            triplet.sendToSrc(triplet.dstAttr._6)
          }
        },
        {
          (a, b) => a ++ b
        }
      )
      vertexRDD = graph.vertices
        //        .partitionBy(new HashPartitioner(partitionNum))
        //        .repartition(partitionNum)
        .leftOuterJoin(msgRDD)
        .map(x =>
          (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, getSetFromOption(x._2._2))))
      //      vertexRDD.foreach(println(_))
      graphWithMsg = Graph(vertexRDD, graph.edges)
      println("[3] 计算合作者增益并更新link分数")
      //计算合作者增益并更新link分数
      graphWithMsg = graphWithMsg.mapTriplets(triplet => {
        //忽略合作者关系边和分数为1的link边，边的属性为2时表示合作者关系 只对link边计算
        if (triplet.attr._1 < 1) {
          val coSim = computeCoauthorSim(triplet.srcAttr._6, triplet.dstAttr._6)
          //使用layer信息作为系数
          //          var sim = triplet.attr._2 + (1 - Weight.alpha) * coSim
          //这个系数alpha?????????
          var sim = triplet.attr._3 + (1 - Weight.alpha) * coSim
          sim = compareSim(sim)
          //          println(sim)
          if (sim == 1.0)
          //            (1.0, 1.0)
            (1.0, 0.0, 0.0)
          else
          //            (sim, triplet.attr._2)
            (sim, triplet.attr._1, triplet.attr._3)
        } else
          triplet.attr
      })

      graph = Graph(graph.vertices, graphWithMsg.edges)
      println("[check]")
      val deltaRDD = graph.edges.map(x => x.attr._1 - x.attr._2).filter(x => x > tolerance && x < 1)
      if (deltaRDD.isEmpty()) {
        check = false
      }

      println("iter:" + iter)
      iter += 1
      println("[checked]")

    }
    println("[End]")
    graph
  }

  /**
    * 不对合作者增益进行迭代更新
    *
    * @param g 作者网络
    */
  def runForTraining(g: Graph[VertexAttr, EdgeML], numPartition: Int): Graph[VertexAttr, EdgeML] = {
    g.cache()
    println("[初始化link相似分数]")
    //第一次更新link分数后的边数组
    var graph = g.mapTriplets(triplet => {
      //边的属性为2时表示合作者关系 只对link边计算
      if (triplet.attr._1 != 2.0) {
        //author_id相同且不等于0(0表示缺省)则返回1(1表示合并节点),否则返回文本相似度
        //        val orgSim = computeOrgSim(triplet.srcAttr._3, triplet.dstAttr._3)
        val orgSim = computeVectorSim(triplet.srcAttr._3, triplet.dstAttr._3)
        val layerSim = computeLayerSim(triplet.srcAttr._4, triplet.dstAttr._4)
        val titleSim = computeVectorSim(triplet.srcAttr._5, triplet.dstAttr._5)
        val abstractSim = computeVectorSim(triplet.srcAttr._6, triplet.dstAttr._6)

        if (triplet.srcAttr._2.equals(triplet.dstAttr._2)) {
          (1.0, layerSim * orgSim, 0.0, layerSim * titleSim, layerSim * abstractSim)
        } else {
          (0.0, layerSim * orgSim, 0.0, layerSim * titleSim, layerSim * abstractSim)
        }
      } else
        (2.0, 0.0, 0.0, 0.0, 0.0)
    })

    println("[1] 将每个节点与其他同名节点的机构相似分数添加到节点中")
    // 把与当前节点相连的link分数传给当前节点
    var msgRDD = graph.aggregateMessages[Set[LinkMsg]](
      triplet => {
        //2表示合作者关系,此处只对link边进行操作
        if (triplet.attr._1 != 2.0) {
          val msg = Set[LinkMsg]((triplet.srcId, triplet.dstId, triplet.attr._2))
          //发送给目的节点
          triplet.sendToDst(msg)
          //发送给源节点
          triplet.sendToSrc(msg)
        }
      },
      {
        //合并link边发来的分数集合
        (a, b) => a ++ b
      }
    )

    /**
      *  x._1: 节点id
      *  x._2._1._1: 作者名
      *  x._2._1._2: 作者id
      *  x._2._1._3: 机构
      *  x._2._1._4: 发表年份
      *  x._2._1._5: 标题向量
      *  x._2._1._6: 摘要向量
      *
      * getSetFromOption(x._2._2): 与当前节点相连的所有link边的信息List[(id1,id2,sim)]
      */
    var vertexRDD = graph.vertices
      //        .partitionBy(new HashPartitioner(partitionNum))
      //        .repartition(partitionNum)
      .leftOuterJoin(msgRDD)
      .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, getSetFromOption(x._2._2))))

    var graphWithMsg = Graph(vertexRDD, graph.edges)
    //  graph.vertices.foreach(println(_))
    println("[2] 将邻居节点与其他重名节点的机构相似分数添加到节点中")
    msgRDD = graphWithMsg.aggregateMessages[Set[LinkMsg]](
      triplet => {
        //2表示合作者关系
        if (triplet.attr._1 == 2.0) {
          //author_id相同，边的属性设为1
          triplet.sendToDst(triplet.srcAttr._7)
          triplet.sendToSrc(triplet.dstAttr._7)
        }
      },
      {
        (a, b) => a ++ b
      }
    )
    vertexRDD = graph.vertices
      //        .partitionBy(new HashPartitioner(partitionNum))
      //        .repartition(partitionNum)
      .leftOuterJoin(msgRDD)
      .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, getSetFromOption(x._2._2))))

    graphWithMsg = Graph(vertexRDD, graph.edges)
    println("[3] 计算合作者增益")
    //计算合作者增益并更新link分数
    graphWithMsg = graphWithMsg.mapTriplets(triplet => {
      //忽略合作者关系边和分数为1的link边，边的属性为2时表示合作者关系 只对link边计算
      if (triplet.attr._1 != 2) {
        val coSim = computeCoauthorSim(triplet.srcAttr._7, triplet.dstAttr._7)
        //val coSim=0.0
        (triplet.attr._1, triplet.attr._2, coSim, triplet.attr._4, triplet.attr._5)
      } else
        triplet.attr
    })
    graph = Graph(graph.vertices, graphWithMsg.edges)
    //    LogTool.log("[End]")
    println("[End]")
    graph
  }

  /**
    * 进行名称去重 迭代退出条件为不超过最大迭代次数
    *
    * @param g             作者网络
    * @param maxIterations 最大迭代次数
    * @param partitionNum  rdd分区数 影响并行计算速度
    */
  def runUntilMaxIter(g: Graph[VertexAttr, EdgeAttr], maxIterations: Double, partitionNum: Int): Graph[VertexAttr, EdgeAttr] = {
    g.cache()
    println("[初始化link相似分数]")
    //第一次更新link分数后的边数组
    var graph = g.mapTriplets(triplet => {
      //边的属性为2时表示合作者关系 只对link边计算
      if (triplet.attr._1 != 2.0) {
        //author_id相同且不等于0(0表示缺省)则返回1(1表示合并节点),否则返回文本相似度
        if (!triplet.srcAttr._2.equals("")
          && !triplet.dstAttr._2.equals("")
          && triplet.srcAttr._2.equals(triplet.dstAttr._2)) {
          (1.0, 0.0, 0.0)
        } else {
          val orgSim = computeVectorSim(triplet.srcAttr._3, triplet.dstAttr._3)
          val titleSim = computeVectorSim(triplet.srcAttr._5, triplet.dstAttr._5)
          val abstractSim = computeVectorSim(triplet.srcAttr._6, triplet.dstAttr._6)
          val layerSim = computeLayerSim(triplet.srcAttr._4, triplet.dstAttr._4)
          val sim = layerSim * Weight.alpha * ((1 - Weight.beta) * titleSim + Weight.beta * orgSim)
          // var sim = Weight.alpha * ((1 - Weight.beta) * textSim + Weight.beta * orgSim)
          (0.0, 0.0, sim)

        }
      } else
        (2.0, 0.0, 0.0)
    })

    var check = true
    var iter = 1
    while (check) {
      println("[1] 将每个节点的link分数添加到节点中")
      // 把与当前节点相连的link分数传给当前节点
      var msgRDD = graph.aggregateMessages[Set[LinkMsg]](
        triplet => {
          //2表示合作者关系,此处只对link边进行操作
          if (triplet.attr._1 != 2.0) {
            val msg = Set[LinkMsg]((triplet.srcId, triplet.dstId, triplet.attr._1))
            //发送给目的节点
            triplet.sendToDst(msg)
            //发送给源节点
            triplet.sendToSrc(msg)
          }
        },
        {
          //合并link边发来的分数集合
          (a, b) => a ++ b
        }
      )

      /**
        *  x._1: 节点id
        *  x._2._1._1: 作者名
        *  x._2._1._2: 向量
        *  x._2._1._3: authorId
        *  x._2._1._4: org
        *  x._2._1._5: year
        * *
        * getSetFromOption(x._2._2): 与当前节点相连的所有link边的信息List[(id1,id2,sim)]
        */
      var vertexRDD = graph.vertices
        //        .partitionBy(new HashPartitioner(partitionNum))
        //        .repartition(partitionNum)
        .leftOuterJoin(msgRDD)
        .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, getSetFromOption(x._2._2))))

      var graphWithMsg = Graph(vertexRDD, graph.edges)
      //  graph.vertices.foreach(println(_))
      println("[2] 将邻居节点的link分数集合添加到当前节点中")
      msgRDD = graphWithMsg.aggregateMessages[Set[LinkMsg]](
        triplet => {
          //2表示合作者关系
          if (triplet.attr._1 == 2.0) {
            //author_id相同，边的属性设为1
            triplet.sendToDst(triplet.srcAttr._6)
            triplet.sendToSrc(triplet.dstAttr._6)
          }
        },
        {
          (a, b) => a ++ b
        }
      )
      vertexRDD = graph.vertices
        //        .partitionBy(new HashPartitioner(partitionNum))
        //        .repartition(partitionNum)
        .leftOuterJoin(msgRDD)
        .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, getSetFromOption(x._2._2))))

      graphWithMsg = Graph(vertexRDD, graph.edges)
      println("[3] 计算合作者增益并更新link分数")
      //计算合作者增益并更新link分数
      graphWithMsg = graphWithMsg.mapTriplets(triplet => {
        //忽略合作者关系边和分数为1的link边，边的属性为2时表示合作者关系 只对link边计算
        if (triplet.attr._1 != 2) {
          val coSim = computeCoauthorSim(triplet.srcAttr._6, triplet.dstAttr._6)
          //使用layer信息作为系数
          //          var sim = triplet.attr._2 + (1 - Weight.alpha) * coSim
          var sim = triplet.attr._3 + (1 - Weight.alpha) * coSim
          sim = compareSim(sim)
          //          println(sim)
          if (sim == 1.0)
          //            (1.0, 1.0)
            (1.0, 0.0, 0.0)
          else
          //            (sim, triplet.attr._2)
            (sim, triplet.attr._1, triplet.attr._3)
        } else
          triplet.attr
      })

      graph = Graph(graph.vertices, graphWithMsg.edges)
      println("[check]")

      if (iter == maxIterations)
        check = false
      iter += 1
      println("iter:" + iter)
      println("[checked]")
    }
    println("[End]")
    graph
  }

  /**
    *
    * @param g            作者网络
    * @param partitionNum rdd分区数
    * @return
    */
  def runML[T<:ClassificationModel[Vector,T]](g: Graph[VertexAttr, EdgeML], partitionNum: Int, model:T): Graph[VertexAttr, EdgeML] = {
    g.cache()
    println("[初始化link相似分数]")
    //第一次更新link分数后的边数组
    var graph = g.mapTriplets(triplet => {
      //边的属性为2时表示合作者关系 只对link边计算
      if (triplet.attr._1 != 2.0) {
        //author_id相同且不等于0(0表示缺省)则返回1(1表示合并节点),否则返回文本相似度
        //judgeAuthorId
        //        if (!triplet.srcAttr._2.equals("")
        //          && !triplet.dstAttr._2.equals("")
        //          && triplet.srcAttr._2.equals(triplet.dstAttr._2)) {
        //          (1.0, 0.0, 0.0)
        //        } else {
        //          val orgSim = computeVectorSim(triplet.srcAttr._3, triplet.dstAttr._3)
        //          val layerSim = computeLayerSim(triplet.srcAttr._4, triplet.dstAttr._4)
        //          val titleSim = computeVectorSim(triplet.srcAttr._5, triplet.dstAttr._5)
        //          val abstractSim = computeVectorSim(triplet.srcAttr._6, triplet.dstAttr._6)
        //          val sim = layerSim * (Weight.wTitle * titleSim + Weight.wOrg * orgSim + Weight.wAbstract * abstractSim)
        //          // var sim = Weight.alpha * ((1 - Weight.beta) * textSim + Weight.beta * orgSim)
        //          (sim, orgSim * layerSim, 0.0, titleSim * layerSim, abstractSim * layerSim)
        //        }

        val orgSim = computeVectorSim(triplet.srcAttr._3, triplet.dstAttr._3)
        val layerSim = computeLayerSim(triplet.srcAttr._4, triplet.dstAttr._4)
        val titleSim = computeVectorSim(triplet.srcAttr._5, triplet.dstAttr._5)
        val abstractSim = computeVectorSim(triplet.srcAttr._6, triplet.dstAttr._6)

        val sim = layerSim * (Weight.wTitle * titleSim + Weight.wOrg * orgSim + Weight.wAbstract * abstractSim)
        // var sim = Weight.alpha * ((1 - Weight.beta) * textSim + Weight.beta * orgSim)
        (0.0, orgSim * layerSim, 0.0, titleSim * layerSim, abstractSim * layerSim)

      } else
        (2.0, 0.0, 0.0, 0.0, 0.0)

    })


    println("[1] 将每个节点的link分数添加到节点中")
    // 把与当前节点相连的link分数传给当前节点
    var msgRDD = graph.aggregateMessages[Set[LinkMsg]](
      triplet => {
        //2表示合作者关系,此处只对link边进行操作
        if (triplet.attr._1 != 2.0) {
          val msg = Set[LinkMsg]((triplet.srcId, triplet.dstId, triplet.attr._2))
          //发送给目的节点
          triplet.sendToDst(msg)
          //发送给源节点
          triplet.sendToSrc(msg)
        }
      },
      {
        //合并link边发来的分数集合
        (a, b) => a ++ b
      }
    )

    /**
      *  x._1: 节点id
      *  x._2._1._1: 作者名
      *  x._2._1._2: 向量
      *  x._2._1._3: authorId
      *  x._2._1._4: org
      *  x._2._1._5: year
      * *
      * getSetFromOption(x._2._2): 与当前节点相连的所有link边的信息List[(id1,id2,sim)]
      */
    var vertexRDD = graph.vertices
      //        .partitionBy(new HashPartitioner(partitionNum))
      //        .repartition(partitionNum)
      .leftOuterJoin(msgRDD)
      .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, getSetFromOption(x._2._2))))

    var graphWithMsg = Graph(vertexRDD, graph.edges)
    //  graph.vertices.foreach(println(_))
    println("[2] 将邻居节点的link分数集合添加到当前节点中")
    msgRDD = graphWithMsg.aggregateMessages[Set[LinkMsg]](
      triplet => {
        //2表示合作者关系
        if (triplet.attr._1 == 2.0) {
          //author_id相同，边的属性设为1
          triplet.sendToDst(triplet.srcAttr._7)
          triplet.sendToSrc(triplet.dstAttr._7)
        }
      },
      {
        (a, b) => a ++ b
      }
    )
    vertexRDD = graph.vertices
      //        .partitionBy(new HashPartitioner(partitionNum))
      //        .repartition(partitionNum)
      .leftOuterJoin(msgRDD)
      .map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, getSetFromOption(x._2._2))))

    graphWithMsg = Graph(vertexRDD, graph.edges)
    println("[3] 计算合作者增益并更新link分数")
    //计算合作者增益并更新link分数
    graphWithMsg = graphWithMsg.mapTriplets(triplet => {
      //忽略合作者关系边和分数为1的link边，边的属性为2时表示合作者关系 只对link边计算
      if (triplet.attr._1 != 2.0) {
        val coSim = computeCoauthorSim(triplet.srcAttr._7, triplet.dstAttr._7)
        //使用layer信息作为系数
        val features = new DenseVector(Array(triplet.attr._2, coSim, triplet.attr._4, triplet.attr._5))
        //        val result = model.transform(ss.parralle())
        //        val result = 1.0
        val result = model.predict(features)
        //println(result)
        (result, triplet.attr._2, coSim, triplet.attr._4, triplet.attr._5)
      } else
        triplet.attr
    })

    graph = Graph(graph.vertices, graphWithMsg.edges)
    println("[End]")
    graph
  }

  /**
    * 保存网络节点和边的rdd到指定路径 仅保留节点id和属性中的作者名字
    *
    * @param graph 作者网络
    * @param path  保存路径
    */
  def save(graph: Graph[VertexAttr, EdgeML], path: String): Unit = {
    val vOut = path + "/out_v"
    val eOut = path + "/out_e"
    graph.vertices.map(x => (x._1, x._2._1))
      .saveAsObjectFile(vOut)
    graph.edges.map(x => Edge(x.srcId, x.dstId, x.attr._1))
      .saveAsObjectFile(eOut)
  }

  /**
    * 保存网络节点和边的rdd到指定路径 仅保留节点id和属性中的作者名字
    *
    * @param graph 作者网络
    * @param path  保存路径
    */
  def saveML(graph: Graph[VertexAttr, EdgeML], path: String): Unit = {
    val vOut = path + "/ml_out_v"
    val eOut = path + "/ml_out_e"
    graph.vertices.map(x => (x._1, x._2._1))
      .saveAsObjectFile(vOut)
    graph.edges.saveAsObjectFile(eOut)

  }

  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val ss: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      .master("local[*]")
      .getOrCreate()
    ss.close
  }

}
