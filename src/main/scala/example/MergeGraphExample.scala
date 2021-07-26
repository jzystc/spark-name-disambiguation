package example

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MergeGraphExample {
  ///media/ubuntu/Ubuntu 18.0/MutipleNetworkNameAmbiguation2
  def mergeGraphs(g1: Graph[String, Int], g2: Graph[String, Int]) = {
    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct.zipWithIndex()

    def edgesWithNewVertexIds(g: Graph[String, Int]) =
      g.triplets
        .map(et => (et.srcAttr, (et.attr, et.dstAttr)))
        .join(v)
        .map(x => ((x._2._1._2), (x._2._2, x._2._1._1)))
        .join(v)
        .map(x => Edge(x._2._1._1, x._2._2, x._2._1._2))

    Graph(v.map(_.swap), edgesWithNewVertexIds(g1).union(edgesWithNewVertexIds((g2))))
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val vertexArray1 = Array(
      (1L, "Alice"),
      (2L, "Bob"),
      (3L, "Charlie"))
    // the Edge class stores a srcId, a dstId and the edge property
    val edgeArray1 = Array(
      Edge(2L, 1L, 1),
      Edge(2L, 3L, 1),
      Edge(3L, 1L, 1))
    // construct the following RDDs from the vertexArray and edgeArray variables.
    val vertexRDD1: RDD[(Long, String)] = sc.parallelize(vertexArray1)
    val edgeRDD1: RDD[Edge[Int]] = sc.parallelize(edgeArray1)

    // build a Property Graph
    val graph1: Graph[String, Int] = Graph(vertexRDD1, edgeRDD1)
    val vertexArray2 = Array(
      (1L, "Alice"),
      (2L, "David"),
      (3L, "Ed"))
    val edgeArray2 = Array(
      Edge(2L, 1L, 1),
      Edge(2L, 3L, 1),
      Edge(3L, 1L, 1))
    // construct the following RDDs from the vertexArray and edgeArray variables.
    val vertexRDD2: RDD[(Long, String)] = sc.parallelize(vertexArray2)
    val edgeRDD2: RDD[Edge[Int]] = sc.parallelize(edgeArray2)
    // build a Property Graph
    val graph2: Graph[String, Int] = Graph(vertexRDD2, edgeRDD2)
    val graph = mergeGraphs(graph1, graph2)
    graph.vertices.collect().foreach(println(_))
    graph.triplets.map(triplet => triplet.srcAttr + "----->" + triplet.dstAttr + "    attr:" + triplet.attr)
      .collect().foreach(println(_))
  }
}
