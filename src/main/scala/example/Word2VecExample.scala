package example

import org.apache.spark.ml.feature.{Tokenizer, Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import util.SimilarityUtil

object Word2VecExample {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Word2Vec example")
      .getOrCreate()
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    val doc1 = spark.createDataFrame(Seq(
      "On the Challenges and Countermeasures in Ideals and Beliefs of Current College Education"
        .split(" ")).map(Tuple1.apply)).toDF("words")
    doc1.show(10)
    val doc2 = spark.createDataFrame(Seq(
      "Photo-produced Hydroxyl Radicals Enhanced by Copper Ions in Aqueous Solution"
        .split(" ")).map(Tuple1.apply)).toDF("words")
    doc2.show(10)
    // val words = tokenizer.transform(doc)
    //val d = tokenizer.transform(doc.filter("text!=\"\""))
    // Learn a mapping from words to Vectors.
    val model = Word2VecModel.load("d:/contest/word2vec_100")

    val v1=model.transform(doc1).rdd.map(x=>x(1).asInstanceOf[DenseVector]).collect()

    val v2 = model.transform(doc2).rdd.map(x=>x(1).asInstanceOf[DenseVector]).collect()
//    print(v1)
   val sim=SimilarityUtil.cosine(v1(0),v2(0))
    println(sim)
//    df1.show(1, truncate = 0)
    //    val word2Vec = new Word2Vec()
    //      .setInputCol("words")
    //      .setOutputCol("result")
    //      .setVectorSize(3)
    //      .setMinCount(0)
    //    val model1 = word2Vec.fit(a)
    //    val model2 = word2Vec.fit(d)

    //    val df1 = model1.transform(a)
    //    val df2 = model2.transform(d)
    //    df1.show()
    //    df2.show()
    spark.stop()
  }
}
