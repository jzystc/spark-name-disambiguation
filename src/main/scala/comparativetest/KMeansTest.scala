package comparativetest

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession
import util.{DBUtil, Training}

object KMeansTest {
  def prepare(): Unit = {

  }

  def run(ss: SparkSession) {
    val dfReader = DBUtil.getDataFrameReader(ss)
    var dataset = dfReader.option("dbtable", "junling_wang").load()
    dataset = Training.fitTfIdf(dataset,"title","titleVec")
    dataset.createOrReplaceTempView("dataset")
    val view = ss.sql("select * from dataset where name = 'junling wang'")
    view.show(5)

    // Loads data.
    //    val dataset = ss.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(43).setSeed(1L)
    val model = kmeans.fit(view)
    // Make predictions
    val predictions = model.transform(dataset)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    //    println("Cluster Centers: ")
    //    model.clusterCenters.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()
    run(ss)
  }
}
