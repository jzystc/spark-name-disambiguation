package util

import org.apache.spark.graphx.Graph

/**
  * 图可视化
  */
object GraphVisualization {
  /**
    * 转换成能用gephi打开的gexf文件
    *
    * @param g 作者网络
    */
  def toGexf[VD, ED](g: Graph[VD, ED]): String = {
    "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "<graph mode=\"static\" defaultedfetype=\"directed\">\n" +
      "<nodes>\n" +
      g.vertices.map(v => "<node id=\"" + v._1 + "\" label=\"" + v._2 + "\" />\n").collect.mkString +
      "</nodes>\n" +
      "<edges>\n" +
      g.edges.map(e => "<edge source=\"" + e.srcId + "\" target=\"" + e.dstId + "\" label=\"" + e.attr + "\" />\n").collect.mkString +
      "</edges>\n" +
      "</graph>\n" +
      "</gexf>"
  }
}
