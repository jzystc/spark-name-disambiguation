package dao

import java.io.{File, PrintWriter}

import entity.Author
import util.DBUtil

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object AuthorDao {

  /**
    * 从author表中根据名字获取所有aid
    *
    * @param name 名字
    * @return aIds:List[Int]
    */
  def getAIdsbyAuthorName(name: String): List[Int] = {
    val conn = DBUtil.getConnection
    try {
      val sql = "SELECT aid FROM authors where name=? limit 10"
      val ps = conn.prepareStatement(sql)
      ps.setObject(1, name)
      val rs = ps.executeQuery()
      var aIds = List[Int]()
      while (rs.next) {
        aIds = rs.getInt("aid") :: aIds
      }
      aIds
    }
    finally {
      conn.close()
    }
  }

  /**
    * 统计author表中某个名字出现的次数
    *
    * @param name 名字
    * @return num:Int
    */
  def getAuthorsNumByName(name: String): Int = {
    val conn = DBUtil.getConnection
    try {
      val sql = "select count(distinct id) as num from authors where name=?"
      val ps = conn.prepareStatement(sql)
      ps.setObject(1, name.replace("_", " "))
      val rs = ps.executeQuery()
      var num = 0
      while (rs.next) {
        num = rs.getInt("num")
      }
      num

    } finally {
      conn.close()
    }
  }

  /**
    * 根据文章id检索相关作者信息
    *
    * @param pId 文章id
    * @return authorList
    */
  def getAuthorsByPaperId(pId: String): List[Author] = {
    // 开启连接
    val conn = DBUtil.getConnection
    try {
      // 对数据的操作声明为只读
      // val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val sql = "SELECT * FROM authors where paper_id = ?"
      val ps = conn.prepareStatement(sql)
      ps.setObject(1, pId)
      val rs = ps.executeQuery()
      // 遍历结果
      var authors: List[Author] = List()
      while (rs.next) {
        val author: Author = new Author()
        author.id = rs.getInt("id")
        author.authorId = rs.getString("author_id")
        author.name = rs.getString("name")
        author.paperId = pId
        author.org = rs.getString("org")
        authors = author :: authors
      }
      authors
    }
    finally {
      conn.close()
    }
  }

  /**
    * 生成一组不重复的随机数
    *
    * @param n 随机数个数
    * @param m 随机数范围
    * @return
    */
  def randomNew(n: Int, m: Int): List[Int] = {
    var arr = 1 to m toArray
    var outList: List[Int] = Nil
    var border = arr.length //随机数范围
    for (_ <- 0 until n) {
      //生成n个数
      val index = (new Random).nextInt(border) + 1
      outList = outList ::: List(arr(index))
      arr(index) = arr.last //将最后一个元素换到刚取走的位置
      arr = arr.dropRight(1) //去除最后一个元素
      border -= 1
    }
    outList
  }

  /**
    * 向指定名称的表中插入与该作者名相关的数据
    *
    * @param name 作者名字
    */
  def insertDataByName(table: String, name: String): Unit = {
    val conn = DBUtil.getConnection
    try {
      val sql = s"insert into ${table}_authors(id,paper_id,name,org,author_id) values(?,?,?,?,?)"
      val ps = conn.prepareStatement(sql)
      val pIds = PaperDao.getPaperIdsByAuthorName(name)
      for (pId <- pIds) {
        val authors = AuthorDao.getAuthorsByPaperId(pId)
        for (author <- authors) {
          ps.setObject(1, author.id)
          ps.setObject(2, author.paperId)
          ps.setObject(3, author.name)
          ps.setObject(4, author.org)
          ps.setObject(5, author.authorId)
          ps.executeUpdate()
        }
      }
    }catch{
      case e:Exception=>
    }
    finally {
      conn.close()
    }
  }

  /**
    * 修复数据库中的问题数据
    * 删除多余空格
    * 更改*为空格
    * 更改.为空格
    */
  def fix(name: String): Unit = {
    val conn = DBUtil.getConnection
    conn.setAutoCommit(false)
    var table="authors"
    if(!name.equals("all")){
      table=name+"_authors"
    }
    try {
      val sql1 = s"update $table set name=replace(name,'.',' ')"
      val sql2 = s"update $table set name=replace(name,'*',' ')"
      val sql3 = s"update $table set name=replace(name,'-',' ')"
      val sql4 = s"update $table set name=replace(name,'  ',' ')"
      val sql5 = s"update $table set name=trim(both from `name`)"
      val ps1 = conn.prepareStatement(sql1)
      val ps2 = conn.prepareStatement(sql2)
      val ps3 = conn.prepareStatement(sql3)
      val ps4 = conn.prepareStatement(sql4)
      val ps5 = conn.prepareStatement(sql5)
      ps1.executeUpdate()
      ps2.executeUpdate()
      ps3.executeUpdate()
      ps4.executeUpdate()
      ps5.executeUpdate()
      conn.commit()
    }
    finally {
      conn.close()
    }
  }

  def createTable(name: String): Unit = {
    val conn = DBUtil.getConnection
    val table = name + "_authors"
    val sql = s"CREATE TABLE $table(" +
      "`id` int(10) NOT NULL PRIMARY KEY," +
      "`paper_id` varchar(100) NOT NULL," +
      "`org` varchar(100) DEFAULT NULL," +
      "`name` varchar(100) DEFAULT NULL," +
      "`author_id` varchar(100) DEFAULT NULL" +
      " ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(sql)
    }catch{
      case e:Exception=>
    }
    finally {
      conn.close()
    }
  }

  /**
    * 随机删除authors表里的id
    *
    */
  def deleteAuthorId(): Unit = {
    //随机删除authors表中的author_id
    val m = 1121831
    val n = (1121831 * 0.5).toInt
    val conn = DBUtil.getConnection
    val sql = "update authors set id=NULL where aid = ?"
    var i = 1
    try {
      while (i < n) {
        val ps = conn.prepareStatement(sql)
        val aid = new Random(System.currentTimeMillis()).nextInt(m) + 1
        ps.setObject(1, aid)
        ps.executeUpdate()
        println(i)
        i += 1
      }
    } finally {
      conn.close()
    }
  }

  /**
    * 将id为空值的数据修改为null
    */
  def updateNullId(): Unit = {
    val conn = DBUtil.getConnection
    val sql = "update authors set id=null where id=''"

    try {

      val ps = conn.prepareStatement(sql)

      ps.executeUpdate()


    } finally {
      conn.close()
    }
  }

  /**
    * 检索数据库中所有名字
    *
    * @return names
    */
  def getAuthorNames: ArrayBuffer[String] = {
    val conn = DBUtil.getConnection
    val sql = "select distinct name from authors"
    try {
      val ps = conn.prepareStatement(sql)
      val rs = ps.executeQuery()
      var names = ArrayBuffer[String]()
      while (rs.next) {

        val name = rs.getString("name")
        names += name
      }
      names
    }
    finally {
      conn.close()
    }
  }

  /**
    * 检索某个名字赌赢的人数
    *
    * @param name 作者名字
    * @return
    */
  def countAuthorByName(name: String): Int = {
    val conn = DBUtil.getConnection
    val sql = "select count(*) as num from authors where name=?"
    val ps = conn.prepareStatement(sql)
    try {
      ps.setObject(1, name)
      val rs = ps.executeQuery()
      var num = 0
      while (rs.next) {
        num = rs.getInt("num")
      }

      num
    } finally {
      conn.close()
    }

  }

  /**
    * 从数据库中检索出现次数大于等于n次的名字
    *
    * @param num 最低出现次数
    * @return
    */
  def getAuthorsByNumLargerThan(num: Int): ArrayBuffer[String] = {

    val conn = DBUtil.getConnection
    var names: ArrayBuffer[String] = new ArrayBuffer[String]()
    val sql = "SELECT name FROM authors GROUP BY `name` HAVING COUNT(`name`)>?"
    val ps = conn.prepareStatement(sql)
    try {
      ps.setObject(1, num)
      val rs = ps.executeQuery()
      while (rs.next) {
        names += rs.getString("name")
      }
      names
    } finally {
      conn.close()
    }
  }

  def getNameShowTimesOver(num: Int, path: String): Int = {
    val writer = new PrintWriter(new File(path))
    val names = getAuthorsByNumLargerThan(num)
    for (name <- names) {
      writer.write(name + '\n')
    }
    writer.close()
    0
  }

  def main(args: Array[String]): Unit = {
    fix("all")
  }
}
