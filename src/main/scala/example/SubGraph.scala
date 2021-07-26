package example

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubGraph {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    // day09-vertices.csv
    // 1,Taro,100
    // 2,Jiro,200
    // 3,Sabo,300
    val vertices = Array[(Long, Int)]((1L, 100), (2L, 200), (3L, 300),(4L,400))
    val v = sc.makeRDD(vertices)
    //    val vertexLines: RDD[String] = sc.textFile("graphdata/day09-vertices.csv")
    //    val v: RDD[(VertexId, (String, Long))] = vertexLines.map(line => {
    //      val cols = line.split(",")
    //      (cols(0).toLong, (cols(1), cols(2).toLong))
    //    })

    // day09-01-edges.csv
    // 1,2,100,2014/12/1
    // 2,3,200,2014/12/2
    // 3,1,300,2014/12/3
    val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
    val edges = Array[Edge[Int]](Edge(1L, 2L, 100), Edge(2L, 3L, 200), Edge(1L, 3L, 300))
    //    val edgeLines: RDD[String] = sc.textFile("graphdata/day09-01-edges.csv")
    //    val e: RDD[Edge[((Long, java.util.Date))]] = edgeLines.map(line => {
    //      val cols = line.split(",")
    //      Edge(cols(0).toLong, cols(1).toLong, (cols(2).toLong, format.parse(cols(3))))
    //    })
    val e = sc.makeRDD(edges)
    val graph: Graph[Int, Int] = Graph(v, e)

    println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
    graph.vertices.collect.foreach(println(_))
    // (2,(Jiro,200))
    // (1,(Taro,100))
    // (3,(Sabo,300))

    println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
    graph.edges.collect.foreach(println(_))
    // Edge(1,2,(100,Mon Dec 01 00:00:00 EST 2014))
    // Edge(2,3,(200,Tue Dec 02 00:00:00 EST 2014))
    // Edge(3,1,(300,Wed Dec 03 00:00:00 EST 2014))

    // reverse ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    println("\n\n~~~~~~~~~ Confirm Edges reversed graph ")
    graph.reverse.edges.collect.foreach(println(_))
    // Edge(2,1,(100,Mon Dec 01 00:00:00 EST 2014))
    // Edge(3,2,(200,Tue Dec 02 00:00:00 EST 2014))
    // Edge(1,3,(300,Wed Dec 03 00:00:00 EST 2014))

    // subgraph ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    println("\n\n~~~~~~~~~ Confirm Subgraphed vertices graph ")
    // 利用subgraph根据顶点和边的条件建立子图
    graph.subgraph(vpred = (vid, v) => v >= 200).vertices.collect.foreach(println(_))
    // (2,(Jiro,200))
    // (3,(Sabo,300))

    println("\n\n~~~~~~~~~ Confirm Subgraphed edges graph ")
    val subg=graph.subgraph(epred = edge => edge.attr >= 200)
    subg.edges.collect.foreach(println(_))
    subg.vertices.collect.foreach(println(_))
    // Edge(2,3,(200,Tue Dec 02 00:00:00 EST 2014))
    // Edge(3,1,(300,Wed Dec 03 00:00:00 EST 2014))

    // 对顶点和边同时加限制
    val subGraph = graph.subgraph(vpred = (vid, v) => v >= 200, epred = edge => edge.attr >= 200)

    println("\n\n~~~~~~~~~ Confirm vertices of Subgraphed graph ")
    subGraph.vertices.collect.foreach(println(_))
    // (2,(Jiro,200))
    // (3,(Sabo,300))


    sc.stop
  }

}
