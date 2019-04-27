package util

import breeze.linalg._
import info.debatty.java.stringsimilarity.{JaroWinkler, Levenshtein, NormalizedLevenshtein}
import org.apache.spark.ml.linalg.Vector

object Similarity {
  /*定义一个全局的用于计算作者所属机构的levenstein编辑距离的类对象*/
  //final val levenshtein = new Levenshtein()

  /*定义一个全局JaroWinkle对象用于计算机构名字符串相似度*/
  final val jaroWinkler = new JaroWinkler()

  /*最大文本相似分数*/
  var maxTextSim = 0.0

  /*最小文本相似分数*/
  var minTextSim = 1.0

  def main(args: Array[String]): Unit = {
    val jaroWinkler = new JaroWinkler()
    val levenshtein = new Levenshtein()
    val normalizedLevenshtein = new NormalizedLevenshtein()

    val org1 = "Institute of Applied Mathematics"
    val org2 = "Department of Electrical and Electronic Engineering, University of Hong Kong"
    val sum = org1.length + org2.length
    val levenDist = levenshtein.distance(org1, org2)
    val levenDist2 = normalizedLevenshtein.distance(org1, org2)
    val levenRatio = (sum - levenDist) / sum
    println("jarowinkler", jaroWinkler.similarity(org1, org2))
    println("levenshtein", levenRatio)
    println("levenshtein normalized", 1 - levenDist2)

  }

  /**
    * 缩放link边的文本相似度分数到[0,1]区间
    *
    */
  def rescaleTextSim(textSim: Double): Double = {
    val scaledTextSim: Double = (textSim - minTextSim) / (maxTextSim - minTextSim)
    scaledTextSim
  }


  /**
    * 判断相似度分数是否大于阈值，大于则返回1，否则返回原值
    *
    * @param sim 相似度分数
    *
    */
  def compareSim(sim: Double): Double = {
    if (sim >= Weight.threshold)
      1.0
    else
      sim
  }

  /**
    * 计算向量余弦相似度
    *
    * @param v1 向量1
    * @param v2 向量2
    */
  def computeVectorSim(v1: Vector, v2: Vector): Double = {
    //    val vs1 = v1.toSparse
    //    val vs2 = v2.toSparse
    //转换成稀疏向量
    //    val breeze1 = new SparseVector(vs1.indices, vs1.values, vs1.size)
    //    val breeze2 = new SparseVector(vs2.indices, vs2.values, vs2.size)
    if (v1.numNonzeros == 0 || v2.numNonzeros == 0) {
      0.0
    }
    else {
      val breeze1 = new DenseVector[Double](v1.toArray)
      val breeze2 = new DenseVector[Double](v2.toArray)
      //计算向量余弦相似度
      val vecSim = breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))
      vecSim
    }
  }

  type LinkMsg = (Long, Long, Double)

  /**
    * 计算合作者关系相似分数
    *
    * @param set1 第1个节点的合作者相似分数集合
    * @param set2 第2个节点的合作者相似分数集合
    */
  def computeCoauthorSim(set1: Set[LinkMsg], set2: Set[LinkMsg]): Double = {
    //两个set中相同的合作者名字个数
    val unionSet = set1 & set2
    //遍历交集,累加每条link边的分数乘以系数后的增益分数
    if (unionSet.nonEmpty) {
      val coefficient = 1.0 / unionSet.size
      val coauthorSim = coefficient * unionSet.map(_._3).sum
      coauthorSim
    }
    else {
      0.0
    }
  }

  //TODO:按照年份划分层次计算相似度
  /**
    * 计算文章出版年份层次相似度
    *
    * @param layer1 paper1的出版年份
    * @param layer2 paper2的出版年份
    * @return 文章出版年份差值对文章相似分数的缩放系数
    */
  def computeLayerSim(layer1: Int, layer2: Int): Double = {
    val delta = (layer1 - layer2).abs.toDouble
    //e^（-|layer1-layer2|/10)
    //Math.exp(-delta / 10.0)
    1.0
  }

  /**
    * 计算机构相似性
    * 基于Jaro-Winkle编辑距离
    *
    * @param org1 第一个机构名
    * @param org2 第二个机构名
    *
    */
  def computeOrgSim(org1: String, org2: String): Double = {
    val orgSim = jaroWinkler.similarity(org1, org2)
    orgSim
  }

}
