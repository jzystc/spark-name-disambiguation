package util

import java.sql.{Connection, DriverManager}

import dao.{AuthorDao, PaperDao}
import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
  * 数据库连接工具
  */
object DBUtil {
  //Class.forName("com.mysql.jdbc.Driver")
  //数据库
  val schema = "scrapy"
  val url = s"jdbc:mysql://localhost:3306/$schema?useSSL=false"
  //用户名
  val username = "root"
  //密码
  val password = "0731"

  /**
    *
    * @param ss 调用该方法的SparkSession
    * @return dateframeReader
    */
  def getDataFrameReader(ss: SparkSession): DataFrameReader = {
    val dfReader = ss.read.format("jdbc")
      .option("url", url).option("driver", "com.mysql.jdbc.Driver")
      .option("user", username)
      .option("password", password)
    dfReader
  }

  /**
    * 获取数据库连接
    */
  def getConnection: Connection = {
    DriverManager.getConnection(url, username, password)
  }

  /**
    * 关闭数据库连接
    *
    * @param conn 数据库连接
    */
  def close(conn: Connection): Unit = {
    try {
      if (!conn.isClosed || conn != null) {
        conn.close()
      }
    }
    catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  /**
    * 创建authors表和papers表
    *
    * @param name 表名前缀
    */
  def createTables(name: String): Unit = {
    PaperDao.createTable(name)
    AuthorDao.createTable(name)
  }

  /**
    *
    * 读取文件中的作者名字,插入与该名字有关的数据到指定表中
    * @param table 表名
    * @param name  作者名
    */
  def insertData(table: String, name: String): Unit = {

    PaperDao.insertDataByName(table = table, name)
    AuthorDao.insertDataByName(table = table, name)

  }

  def main(args: Array[String]): Unit = {

    val name = "c_y_huang"
    createTables(name)
    insertData(name, name)
  }

}

