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
    *
    * 读取文件中的作者名字,插入与这些名字有关的而数据到数据库中
    */
  def insertData(names: Array[String]): Unit = {
    for (name <- names) {
      PaperDao.createTable(name)
      PaperDao.insertDataByName(table = name, name)
      AuthorDao.createTable(name)
      AuthorDao.insertDataByName(table = name, name)
    }
  }

  def main(args: Array[String]): Unit = {
    insertData(Array("j_yu"))
  }

}

