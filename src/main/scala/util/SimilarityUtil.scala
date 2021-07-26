package util

import breeze.linalg._
//import info.debatty.java.stringsimilarity.{JaroWinkler, Levenshtein, NormalizedLevenshtein}
import org.apache.spark.ml.linalg.Vector

object SimilarityUtil {
  //  val minYear = 0
  //  val maxYear = 20
  //  val maxYearDelta: Int = maxYear - minYear
  val maxYearDelta: Int = 20
  /*定义一个全局的用于计算作者所属机构的levenstein编辑距离的类对象*/
  //final val levenshtein = new Levenshtein()
  //  def testEditDistance(): Unit = {
  //    val jaroWinkler = new JaroWinkler()
  //    val levenshtein = new Levenshtein()
  //    val normalizedLevenshtein = new NormalizedLevenshtein()
  //    val org1 = "Institute of Applied Mathematics"
  //    val org2 = "Department of Electrical and Electronic Engineering, University of Hong Kong"
  //    val sum = org1.length + org2.length
  //    val levenDist = levenshtein.distance(org1, org2)
  //    val levenDist2 = normalizedLevenshtein.distance(org1, org2)
  //    val levenRatio = (sum - levenDist) / sum
  //    println("jarowinkler", jaroWinkler.similarity(org1, org2))
  //    println("levenshtein", levenRatio)
  //    println("levenshtein normalized", 1 - levenDist2)
  //  }

  def computeOrgSim(org1: Array[String], org2: Array[String]): Double = {
    val orgSet1 = org1.toSet
    val orgSet2 = org2.toSet
    val intersection = orgSet1 & orgSet2
    if (intersection.size == orgSet1.size || intersection.size == orgSet2.size) {
      1
    } else {
      Math.min(1, intersection.size * 0.2)
    }
  }

  def jaccard(arr1: Array[String], arr2: Array[String]): Double = {
    val set1 = arr1.toSet
    val set2 = arr2.toSet
    //    print(set1,set2)
    if (set1.isEmpty || set2.isEmpty) {
      0.0
    } else {
      var intersectionSize = (set1 & set2).size
      if (intersectionSize == 0) {
        0.0
      } else {
        intersectionSize -= 1
        val unionSize = (set1.size + set2.size)- 1 - intersectionSize
        val jaccard = 1.0 * intersectionSize / unionSize
        jaccard
      }
    }

  }

  /**
   * 计算向量余弦相似度
   *
   * @param v1 向量1
   * @param v2 向量2
   */
  def cosine(v1: Vector, v2: Vector): Double = {
    //    val vs1 = v1.toSparse
    //    val vs2 = v2.toSparse
    //转换成稀疏向量
    //    val breeze1 = new SparseVector(vs1.indices, vs1.values, vs1.size)
    //    val breeze2 = new SparseVector(vs2.indices, vs2.values, vs2.size)
    //    if (v1 == null || v2 == null) {
    //      return 0.0
    //    }
    if (v1.numNonzeros == 0 || v2.numNonzeros == 0) {
      0.0
    }
    else {
      val breeze1 = new DenseVector[Double](v1.toArray)
      val breeze2 = new DenseVector[Double](v2.toArray)
      //计算向量余弦相似度
      val vecSim = breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))
      math.max(0.0, vecSim)
    }
  }


  def computeCoauthorSim(names1: Array[String], names2: Array[String]): Double = {
    //两个set中相同的合作者名字个数
    val unionSet = names1.toSet & names2.toSet
    //遍历交集,累加每条link边的分数乘以系数后的增益分数
    if (unionSet.nonEmpty) {
      val coauthorSim = Math.min(1.0, 0.3 * (unionSet.size - 1))
      coauthorSim
    }
    else {
      0.0
    }
  }

  /**
   * 计算文章出版年份层次相似度
   *
   * @param year1 paper1的出版年份
   * @param year2 paper2的出版年份
   * @return 文章出版年份差值对文章相似分数的缩放系数
   */
  def computeYearSim(year1: Integer, year2: Integer): Double = {

    val delta = (year1 - year2).abs.toDouble
    //e^（-|layer1-layer2|/10)
    Math.exp(-delta / 10.0)
    //1.0
  }

  /**
   * differnce of publication year of two citations with normalization
   *
   * @param year1
   * @param year2
   * @return
   */
  def yearDifference(year1: Int, year2: Int): Double = {
    (year1 - year2).abs.toDouble / maxYearDelta
  }

  def main(args: Array[String]): Unit = {
    //    val r=jaccard(Array("nanjing","university"),Array("beijing","university"))
//    val org1 = "Department of Internal Medicine, Aoto Hospital, Jikei University School of Medicine, Tokyo, Japan"
//    val org2 = "jikei university school of medicine"
//    val sim = jaccard(StringUtil.cleanOrg(org1).split(" "), StringUtil.cleanOrg(org2).split(" "))
//    println(sim)
    println(1/2)
    println(1.0*1/2)
  }
}
