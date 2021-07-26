package util

import java.io._

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.slf4j.LoggerFactory

object JsonUtil {

  class Tuple2(a: Long, b: Long) {
    var x: Long = a
    var y: Long = b
  }

  private val LOGGER = LoggerFactory.getLogger(classOf[Nothing])

  def loadJsonArray(filePath: String): JSONArray = {
    val file = new File(filePath)
    val bytes = new Array[Byte](file.length().toInt)
    val fileInputStream = new FileInputStream(filePath)
    val ret = fileInputStream.read(bytes)
    val content = new String(bytes, 0, ret)
    val jsonArray = JSON.parseArray(content)
    jsonArray
  }

  def loadJson(filePath: String): JSONObject = {
    val file = new File(filePath)
    val bytes = new Array[Byte](file.length().toInt)
    val fileInputStream = new FileInputStream(filePath)
    val ret = fileInputStream.read(bytes)
    val content = new String(bytes, 0, ret)
    val jsonObject = JSON.parseObject(content)
    jsonObject
  }

  // 将class对象保存到本地json文件中
  def saveJson(json: JSONObject, filePath: String): Unit = {

    val jsonString = json.toString()
    //    LOGGER.info(jsonString)

    val file = new File(filePath)
    //如果文件不存在则新建
    if (!file.exists) try file.createNewFile()
    catch {
      case e: IOException =>
        LOGGER.error(e.getMessage)
    }
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false), "UTF-8"))
    //如果多次执行同一个流程，会导致json文件内容不断追加，在写入之前清空文件
    try {
      writer.write("")
      writer.write(jsonString)
    } catch {
      case e: IOException =>
        LOGGER.error(e.getMessage)
    } finally {
      try {
        if (writer != null) {
          writer.close()
        }
      } catch {
        case e: IOException =>
          LOGGER.error(e.getMessage)
      }
    }

  }
}