package util

import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

object DataPreparation {

  def prepare(ss: SparkSession, path: String, name: String): Unit = {
    //    import spark.implicits._
    //获取dataframe读取器 从数据库中读取数据为dataframe
    val dfReader = DBUtil.getDataFrameReader(ss)
    var authors: String = "authors"
    var papers: String = "papers"
    if (name != "") {
      authors = name + "_authors"
      papers = name + "_papers"
    }
    val authorsDF = dfReader.option("dbtable", authors).load()
    val papersDF = dfReader.option("dbtable", papers).load()
    //  table authors join papers by paper_id, and delete column "paper_id".
    var vertexDF = authorsDF.join(papersDF,
      authorsDF("paper_id") === papersDF("paper_id"), "inner")
      .toDF()
      .drop("paper_id")
    val lAuthorsDF = authorsDF.withColumnRenamed("id", "srcId")
    //change column name
    //    authorsDF.createOrReplaceTempView("authortemp")
    val rAuthorsDF = authorsDF.withColumnRenamed("id", "dstId")
    //    val edgeDF = ss.sql("select authortemp.id,authortemp.id from authortemp where authortemp.paper_id = authortemp.paper_id and authortemp.id != authortemp.id" ).show(10)
    //构建link边dataframe
    var linkDF = lAuthorsDF.join(rAuthorsDF,
      //条件1 作者名相同
      lAuthorsDF("name") === rAuthorsDF("name")
        //条件2 srcId < dstId 避免重复组合
        && lAuthorsDF("srcId") < rAuthorsDF("dstId")
        //条件3 paper_id不同
        && lAuthorsDF("paper_id") =!= rAuthorsDF("paper_id"), "inner")
      //删除重复列
      .drop("paper_id", "name", "org", "id")

    //生成第一个边属性
    linkDF = linkDF.withColumn("label", linkDF("srcId") * 0.0)
    var coauthorDF = lAuthorsDF.join(rAuthorsDF,
      lAuthorsDF("paper_id") === rAuthorsDF("paper_id") && lAuthorsDF("srcId") < rAuthorsDF("dstId"), "inner")
      .drop("paper_id", "name", "org", "id")

    coauthorDF = coauthorDF.withColumn("label", coauthorDF("srcId") * 0.0 + 2.0)

    var edgeDF = coauthorDF.union(linkDF)
    edgeDF = edgeDF.withColumn("orgSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("coauthorSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("titleSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("abstractSim", edgeDF("label") * 0.0)

    //使用spark的tfidf模型对初始vertexDF中的标题文本进行训练,返回一个新的dataframe,字符串转换为向量表示
    vertexDF = Training.fitWord2Vec(vertexDF, "org", "orgVec")
    vertexDF = Training.fitWord2Vec(vertexDF, "title", "titleVec")
    vertexDF = Training.fitWord2Vec(vertexDF, "abstract", "abstractVec")
      .drop("paper_id", "venue")
    //保存节点和边dataframe为parquet文件到磁盘上
    //vertex dataframe 导出路径
    val vp = path + name + "/input_v"
    //edge dataframe 导出路径
    val ep = path + name + "/input_e"
    //写入parquet文件到指定路径
    vertexDF.write.parquet(vp)
    edgeDF.write.parquet(ep)
  }

  /**
    * 从数据库中读取数据构建作者网络的节点和边dataframe并保存到磁盘的指定位置
    *
    * @param name 数据库表名
    */
  def prepareByName(ss: SparkSession, name: String, path: String) {

    //获取dataframe读取器 从数据库中读取数据为dataframe
    val dfReader = DBUtil.getDataFrameReader(ss)
    //读取指定表中的数据
    var vertexDF = dfReader.option("dbtable", name).load()

    //读取表中的数据并将id列重命名为srcId
    val lVertexDF = vertexDF.withColumnRenamed("id", "srcId")
    //读取表中的数据并将id列重命名为dstId
    val rVertexDF = vertexDF.withColumnRenamed("id", "dstId")

    //构建link边dataframe
    var linkDF = lVertexDF.join(rVertexDF,
      //条件1 作者名相同
      lVertexDF("name") === rVertexDF("name")
        //条件2 srcId < dstId 避免重复组合
        && lVertexDF("srcId") < rVertexDF("dstId")
        //条件3 paper_id不同
        && lVertexDF("paper_id") =!= rVertexDF("paper_id"), "inner")
      //删除重复列
      .drop("paper_id", "name", "org", "author_id", "title", "abstract", "venue", "year", "keywords")
    //生成第一个边属性
    linkDF = linkDF.withColumn("label", linkDF("srcId") * 0.0)

    //合作者关系dataframe
    var coauthorDF = lVertexDF.join(rVertexDF,
      lVertexDF("paper_id") === rVertexDF("paper_id") && lVertexDF("srcId") < rVertexDF("dstId"), "inner")
      .drop("paper_id", "name", "org", "author_id", "title", "abstract", "venue", "year", "keywords")
    coauthorDF = coauthorDF.withColumn("label", coauthorDF("srcId") * 0.0 + 2.0)

    var edgeDF = coauthorDF.union(linkDF)
    edgeDF = edgeDF.withColumn("oldLabel", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("orgSim", edgeDF("label") * 0.0)

    //使用spark的tfidf模型对初始vertexDF中的标题文本进行训练,返回一个新的dataframe,字符串转换为向量表示
    //    vertexDF = Training.fitTfIdf(vertexDF, "title", "titleVec")
    vertexDF = Training.fitWord2Vec(vertexDF, "title", "titleVec")
    vertexDF = Training.fitWord2Vec(vertexDF, "org", "orgVec")
    vertexDF = Training.fitWord2Vec(vertexDF, "abstract", "abstractVec")
      .drop("paper_id", "venue")

    //保存节点和边dataframe为parquet文件到磁盘上
    //vertex dataframe 导出路径
    val vp = path + name + "/input_v"
    //edge dataframe 导出路径
    val ep = path + name + "/input_e"
    //写入parquet文件到指定路径
    vertexDF.write.parquet(vp)
    edgeDF.write.parquet(ep)
    vertexDF.show(1)
    edgeDF.show(1)
  }

  def prepareML(ss: SparkSession, name: String, path: String, model: Word2VecModel): Unit = {
    //    import spark.implicits._
    //获取dataframe读取器 从数据库中读取数据为dataframe
    var authors: String = "authors"
    var papers: String = "papers"
    if (name != "") {
      authors = name + "_authors"
      papers = name + "_papers"
    }

    val dfReader = DBUtil.getDataFrameReader(ss)
    val authorsDF = dfReader.option("dbtable", authors).load()
    val papersDF = dfReader.option("dbtable", papers).load()
    //  table authors join papers by paper_id, and delete column "paper_id".
    var vertexDF = authorsDF.join(papersDF,
      authorsDF("paper_id") === papersDF("paper_id"), "inner")
      .toDF()
      .drop("paper_id")
    val lAuthorsDF = authorsDF.withColumnRenamed("id", "srcId")
    //change column name
    //    authorsDF.createOrReplaceTempView("authortemp")
    val rAuthorsDF = authorsDF.withColumnRenamed("id", "dstId")
    //    val edgeDF = ss.sql("select authortemp.id,authortemp.id from authortemp where authortemp.paper_id = authortemp.paper_id and authortemp.id != authortemp.id" ).show(10)
    //构建link边dataframe
    var linkDF = lAuthorsDF.join(rAuthorsDF,
      //条件1 作者名相同
      lAuthorsDF("name") === rAuthorsDF("name")
        //条件2 srcId < dstId 避免重复组合
        && lAuthorsDF("srcId") < rAuthorsDF("dstId")
        //条件3 paper_id不同
        && lAuthorsDF("paper_id") =!= rAuthorsDF("paper_id"), "inner")
      //删除重复列
      .drop("paper_id", "name", "org", "author_id")

    //生成第一个边属性
    linkDF = linkDF.withColumn("label", linkDF("srcId") * 0.0)
    var coauthorDF = lAuthorsDF.join(rAuthorsDF,
      lAuthorsDF("paper_id") === rAuthorsDF("paper_id") && lAuthorsDF("srcId") < rAuthorsDF("dstId"), "inner")
      .drop("paper_id", "name", "org", "author_id")

    coauthorDF = coauthorDF.withColumn("label", coauthorDF("srcId") * 0.0 + 2.0)

    var edgeDF = coauthorDF.union(linkDF)
    edgeDF = edgeDF.withColumn("orgSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("coauthorSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("titleSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("abstractSim", edgeDF("label") * 0.0)

    //使用spark的tfidf模型对初始vertexDF中的标题文本进行训练,返回一个新的dataframe,字符串转换为向量表示
    //    vertexDF = Training.fitTfIdf(vertexDF, "title", "titleVec")
    //TODO:合并abstract和title

    /*import org.apache.spark.sql.functions._
       vertexDF.na.fill("",Array("abstract","title"))
       vertexDF = vertexDF.select(concat_ws(",", vertexDF("title"),vertexDF("abstrct")).cast(StringType).as("text"))
       vertexDF = GlobalTraining.trans(vertexDF, "text", "textVec", model)
     */
    vertexDF = GlobalTraining.trans(vertexDF, "org", "orgVec", model)
    vertexDF = GlobalTraining.trans(vertexDF, "abstract", "abstractVec", model)
    vertexDF = GlobalTraining.trans(vertexDF, "title", "titleVec", model)
    //保存节点和边dataframe为parquet文件到磁盘上
    //vertex dataframe 导出路径
    val vp = path + name + "/ml_input_v"
    //edge dataframe 导出路径
    val ep = path + name + "/ml_input_e"
    //写入parquet文件到指定路径
    vertexDF.show(1)
    edgeDF.show(1)
    vertexDF.write.parquet(vp)
    edgeDF.write.parquet(ep)
  }

  /**
    * 从数据库中读取数据构建用于构建逻辑回归模型的作者网络的节点和边dataframe并保存到磁盘的指定位置
    *
    * @param name 数据库表名
    */
  def prepareMLByName(ss: SparkSession, name: String, path: String, model: Word2VecModel) {

    //获取dataframe读取器 从数据库中读取数据为dataframe
    val dfReader = DBUtil.getDataFrameReader(ss)
    //读取指定表中的数据
    var vertexDF = dfReader.option("dbtable", name).load()
    //    tabelDF.createOrReplaceTempView("table")
    //    var vertexDF=ss.sql("select id,name,id,org,paper_id,title,year,abstract from table")
    //读取表中的数据并将id列重命名为srcId
    val lVertexDF = vertexDF.withColumnRenamed("id", "srcId")
    //读取表中的数据并将id列重命名为dstId
    val rVertexDF = vertexDF.withColumnRenamed("id", "dstId")

    //构建link边dataframe
    var linkDF = lVertexDF.join(rVertexDF,
      //条件1 作者名相同
      lVertexDF("name") === rVertexDF("name")
        //条件2 srcId < dstId 避免重复组合
        && lVertexDF("srcId") < rVertexDF("dstId")
        //条件3 paper_id不同
        && lVertexDF("paper_id") =!= rVertexDF("paper_id"), "inner")
      //删除重复列
      .drop("paper_id", "name", "org", "venue", "author_id", "title", "abstract", "year", "keywords")
    //生成第一个边属性
    linkDF = linkDF.withColumn("label", linkDF("srcId") * 0.0)

    //合作者关系dataframe
    var coauthorDF = lVertexDF.join(rVertexDF,
      lVertexDF("paper_id") === rVertexDF("paper_id") && lVertexDF("srcId") < rVertexDF("dstId"), "inner")
      .drop("paper_id", "name", "org", "author_id", "venue", "title", "abstract", "year", "keywords")
    coauthorDF = coauthorDF.withColumn("label", coauthorDF("srcId") * 0.0 + 2.0)

    var edgeDF = coauthorDF.union(linkDF)
    edgeDF = edgeDF.withColumn("orgSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("coauthorSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("titleSim", edgeDF("label") * 0.0)
    edgeDF = edgeDF.withColumn("abstractSim", edgeDF("label") * 0.0)

    //使用spark的tfidf模型对初始vertexDF中的标题文本进行训练,返回一个新的dataframe,字符串转换为向量表示
    //    vertexDF = Training.fitTfIdf(vertexDF, "title", "titleVec")

    vertexDF = GlobalTraining.trans(vertexDF, "org", "orgVec", model)
    vertexDF = GlobalTraining.trans(vertexDF, "abstract", "abstractVec", model)
    vertexDF = GlobalTraining.trans(vertexDF, "title", "titleVec", model)
    //    vertexDF = Training.fitWord2Vec(vertexDF, "org", "orgVec")
    //    vertexDF = Training.fitWord2Vec(vertexDF, "title", "titleVec")
    //    vertexDF = Training.fitWord2Vec(vertexDF, "abstract", "abstractVec")
    //保存节点和边dataframe为parquet文件到磁盘上
    //vertex dataframe 导出路径
    val vp = path + name + "/ml_input_v"
    //edge dataframe 导出路径
    val ep = path + name + "/ml_input_e"
    //写入parquet文件到指定路径
    vertexDF.write.parquet(vp)
    edgeDF.write.parquet(ep)
    vertexDF.show(1)
    edgeDF.show(1)
  }


  def main(args: Array[String]): Unit = {
    //初始化SparkSession
    val ss: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      .master("local[20]")
      .getOrCreate()

    //数据库表名
    val name = "xu_xu"
    val path = "d:/"

    //prepareMLByName(ss, name, path)
    //prepareByName(ss, name, path)
    //prepare(ss, path)
    ss.close()
  }
}
