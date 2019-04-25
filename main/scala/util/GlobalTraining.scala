package util

import org.apache.spark.ml.feature.{RegexTokenizer, Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GlobalTraining {
  def getRowText(ss: SparkSession): DataFrame = {
    val dfReader = DBUtil.getDataFrameReader(ss)
    val authorsDF = dfReader.option("dbtable", "authors").load()
    val papersDF = dfReader.option("dbtable", "papers") load()
    authorsDF.createTempView("authors")
    papersDF.createTempView("papers")
    val org = ss.sql("select org from authors").toDF("text")
    import ss.sqlContext.implicits._
    val titleAbstract = ss.sql("select title,abstract from papers")
    //迭代"连接"/"合并"文本,","隔开???空格是不是更合适" ".
    val mergedTitleAbstract = titleAbstract.map(_.toSeq.foldLeft("")(_ + "," + _).substring(1)).toDF("text")
    //union完成DataFrame合并
    val text = org.union(mergedTitleAbstract).filter("text!=\"\"")
    //RegexTokenizer基于正则表达式.setPattern(re),设置分词
    //val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    //分词结果存储到dataframe
    text
  }

  def trainWord2Vec(ss: SparkSession, text: DataFrame, vectorSize: Int = 100, minCount: Int = 0, modelSavePath: String): Unit = {

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern("\\W")

    val words = regexTokenizer.transform(text).drop("text")

    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("features")
      .setVectorSize(vectorSize)
      .setMinCount(minCount)

    val model = word2Vec.fit(words)
    val modelName = s"word2vec_$vectorSize"
    model.write.overwrite().save(modelSavePath + "/" + modelName)
  }

  def loadWord2VecModel(path: String): Word2VecModel = {
    val model = Word2VecModel.load(path)
    model
  }

  def trans(data: DataFrame, inputCol: String, outputCol: String, model: Word2VecModel): DataFrame = {
    //val tokenizer = new Tokenizer().setInputCol(inputCol).setOutputCol("words")
    //分词结果存储到dataframe
    val regexTokenizer = new RegexTokenizer().setInputCol(inputCol).setOutputCol("words")
    val words = regexTokenizer.transform(data.na.fill("", Array(inputCol))).drop(inputCol)
    model.setOutputCol(outputCol)
    val result = model.transform(words)
    result.drop("words")
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    val path = "/home/csubigdata/namedis/"
    //val text=getRowText(ss)
    //text.write.parquet(path+"rowtext")
    val rowText = ss.read.parquet(path + "rowtext")
    //rowText.repartition(400)
    val vectorSizeArray = Array[Int](100, 150, 200)
    for (size <- vectorSizeArray) {
      trainWord2Vec(ss, rowText, size, 0, path)
      println(s"size:$size")
    }
    //val model = loadWord2VecModel(path)
    //model.getVectors.show()
    ss.close()
  }
}
