package example

import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MakeGraphExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    // [A] creating the Property Graph from arrays of vertices and edges
    println("[A] creating the Property Graph from arrays of vertices and edges");
    // Each vertex is keyed by a unique 64-bit long identifier (VertexID), like '1L'
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50)))
    // the Edge class stores a srcId, a dstId and the edge property
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3))

    // construct the following RDDs from the vertexArray and edgeArray variables.
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    // build a Property Graph
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    // [B] Extract the vertex and edge RDD views of a graph
    println("[B] Extract the vertex and edge RDD views of a graph");
    // Solution 1
    println("Solution 1:============")
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    // Solution 2
    println("Solution 2:============")
    graph.vertices.filter(v => v._2._2 > 30).collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

    // Solution 3
    println("Solution 3:============")
    for ((id, (name, age)) <- graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect) {
      println(s"$name is $age")
    }

    // [C] Exposes a triplet view which logically joins the vertex and edge properties yielding an RDD[EdgeTriplet[VD, ED]]
    println("[C] Exposes a triplet view which logically joins the vertex and edge properties yielding an RDD[EdgeTriplet[VD, ED]]");
    println("Use the graph.triplets view to display who likes who: ")
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    // For extra credit, find the lovers.
    // If someone likes someone else more than 5 times than that relationship is getting pretty serious.
    println("For extra credit, find the lovers if has:============")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
    }

    // [D] Graph Operators
    // Property Graphs also have a collection of basic operations
    println("[D] Graph Operators")

    // compute the in-degree of each vertex
    val inDegrees: VertexRDD[Int] = graph.inDegrees

    // Define a class to more clearly model the user property
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
    // Create a user Graph
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }

    // Fill in the degree information
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }

    // Here we use the outerJoinVertices method of Graph which has the following (confusing) type signature:
    // def outerJoinVertices[U, VD2](other: RDD[(VertexID, U)])(mapFunc: (VertexID, VD, Option[U]) => VD2): Graph[VD2, ED]

    // Using the degreeGraph print the number of people who like each user:
    println("Using the degreeGraph print the number of people who like each user:============")
    for ((id, property) <- userGraph.vertices.collect) {
      println(s"User $id is called ${property.name} and is liked by ${property.inDeg} people.")
    }

    // Print the names of the users who are liked by the same number of people they like.
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }

    // [D.1] The Map Reduce Triplets Operator
    // The mapReduceTriplets operator enables neighborhood aggregation and find the oldest follower of each user
    println("[D.1] The Map Reduce Triplets Operator")
    // Find the oldest follower for each user
    println("Find the oldest follower for each user:============")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
      // For each edge send a message to the destination vertex with the attribute of the source vertex
      edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
      // To combine messages take the message for the older follower
      (a, b) => if (a._2 > b._2) a else b)
    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str) }

    // Try finding the average follower age of the followers of each user
    println("Try finding the average follower age of the followers of each user:============")
    val averageAge: VertexRDD[Double] = userGraph.aggregateMessages[(Int, Double)](
      // map function returns a tuple of (1, Age)
      edge => Iterator((edge.dstId, (1, edge.srcAttr.age.toDouble))),
      // reduce function combines (sumOfFollowers, sumOfAge)
      (a, b) => ((a._1 + b._1), (a._2 + b._2))).mapValues((id, p) => p._2 / p._1)

    //     Display the results
    userGraph.vertices.leftJoin(averageAge) { (id, user, optAverageAge) =>
      optAverageAge match {
        case None => s"${user.name} does not have any followers."
        case Some(avgAge) => s"The average age of ${user.name}\'s followers is $avgAge."
      }
    }.collect.foreach { case (id, str) => println(str) }

    //     [D.2] Subgraph
    //     The subgraph operator that takes vertex and edge predicates and returns the graph
    //     containing only the vertices that satisfy the vertex predicate (evaluate to true)
    //     and edges that satisfy the edge predicate and connect vertices that satisfy the
    //     vertex predicate.
    println("[D.2] Subgraph")
    // restrict our graph to the users that are 30 or older
    println("restrict our graph to the users that are 30 or older:============")
    val olderGraph = userGraph.subgraph(vpred = (id, user) => user.age >= 30)
    // compute the connected components
    val cc = olderGraph.connectedComponents
    // display the component id of each user:
    olderGraph.vertices.leftJoin(cc.vertices) {
      case (id, user, comp) => s"${user.name} is in component ${comp.get}"
    }.collect.foreach { case (id, str) => println(str) }

    val newGraph = userGraph.pageRank(tol = 0.001, resetProb = 0.1)
    val v = newGraph.vertices
    print(v.reduce((a, b) => if (a._2 > b._2) a else b))

  }
}
