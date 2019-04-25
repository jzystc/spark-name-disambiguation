package dao

import entity.Paper
import util.DBUtil

import scala.io.Source

object PaperDao {
  /**
    * 根据作者名检索paper_id
    *
    * @param name 作者名字
    * @return pIds:List[String]
    */
  def getPaperIdsByAuthorName(name: String): List[String] = {
    val conn = DBUtil.getConnection
    try {
      val sql = "SELECT paper_id FROM authors where name = ?"
      val ps = conn.prepareStatement(sql)
      ps.setObject(1, name.replace("_"," "))
      val rs = ps.executeQuery()
      var pIds = List[String]()
      while (rs.next) {
        pIds = rs.getString("paper_id") :: pIds
      }
      //去重
      pIds.distinct
    }
    finally {
      conn.close()
    }
  }

  /**
    * 根据paper_id 检索文章
    *
    * @param pId 文章id
    * @return
    */
  def getPaperByPaperId(pId: String): Paper = {
    // 开启连接
    val conn = DBUtil.getConnection
    try {
      // 对数据的操作声明为只读
      //      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // 查询
      val sql = "SELECT * FROM papers where paper_id=?"
      val ps = conn.prepareStatement(sql)
      ps.setObject(1, pId)
      val rs = ps.executeQuery()
      val paper = new Paper()
      while (rs.next) {
        paper.title = rs.getString("title")
        paper.pAbstract = rs.getString("abstract")
        paper.year = rs.getInt("year")
        paper.venue = rs.getString("venue")
        paper.paperId = rs.getString("paper_id")
      }
      paper
    }
    finally {
      conn.close()
    }
  }

  /**
    * 根据作者名字查询相关文章数据
    *
    * @param name 作者名字
    * @return paperList
    */
  def getPapersByAuthorName(name: String): List[Paper] = {
    // 开启连接
    val conn = DBUtil.getConnection
    try {
      // 对数据的操作声明为只读
      //      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // 查询
      val sql = "SELECT * FROM authors,papers where authors.paper_id=papers.paper_id and authors.name like ?"
      val ps = conn.prepareStatement(sql)
      ps.setObject(1, name)
      val rs = ps.executeQuery()
      var papers: List[Paper] = List()
      while (rs.next) {
        val paper = new Paper()
        paper.title = rs.getString("title")
        paper.pAbstract = rs.getString("abstract")
        paper.year = rs.getInt("year")
        paper.venue = rs.getString("venue")
        paper.paperId = rs.getString("paper_id")
        papers = paper :: papers
      }
      papers
    }
    finally {
      conn.close()
    }
  }

  /**
    * 向指定名称的表中插入与该作者名相关的数据
    *
    * @param name 作者名字
    */
  def insertDataByName(table: String, name: String): Unit = {
    val conn = DBUtil.getConnection
    try {
      val sql = s"insert into ${table}_papers(paper_id,title,year,abstract,venue) values(?,?,?,?,?)"
      val ps = conn.prepareStatement(sql)
      val pIds = PaperDao.getPaperIdsByAuthorName(name)
      for (pId <- pIds) {
        val paper = PaperDao.getPaperByPaperId(pId)
        ps.setObject(1, paper.paperId)
        ps.setObject(2, paper.title)
        ps.setObject(3, paper.year)
        ps.setObject(4, paper.pAbstract)
        ps.setObject(5, paper.venue)
        ps.executeUpdate()
      }
    }
    /*catch {
      case _: com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException =>
      case ex: IOException => ex.printStackTrace()
    }*/
    finally {
      conn.close()
    }
  }

  /**
    * 创建名称为name的表
    *
    * @param name 表名
    */
  def createTable(name: String): Unit = {
    val conn = DBUtil.getConnection
    val table = name + "_papers"
    val sql = s"CREATE TABLE $table (" +
      "`paper_id` varchar(100) UNIQUE NOT NULL PRIMARY KEY," +
      "`title` varchar(100) DEFAULT NULL," +
      "`abstract` varchar(10000) DEFAULT NULL," +
      "`venue` varchar(10000) DEFAULT NULL," +
      "`year` int(10) DEFAULT NULL" +
      " ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(sql)
    }
    finally {
      conn.close()
    }
  }

  /**
    * 将单独的作者表数据汇总到指定的表中
    */
  def union(mainTable: String): Unit = {
    //    val file = Source.fromFile(System.getProperty("user.dir") + "/src/main/resources/namelist.txt")
    val file = Source.fromFile(System.getProperty("user.dir") + "/src/main/resources/18.txt")
    val names = file.getLines()
    val conn = DBUtil.getConnection
    try {
      for (name <- names) {
        println(name)
        val subTable = name
        val sql = s"insert into $mainTable select * from " +
          s"$subTable where not exists (select * from test where test.aid=table.aid)"
        val ps = conn.prepareStatement(sql)
        ps.executeUpdate()
      }
    } finally {
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {
    val name = "jian_du"
    createTable(name)
    insertDataByName(table = name, name)
  }
}
