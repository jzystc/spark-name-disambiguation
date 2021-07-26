package util

import java.nio.charset.Charset

import com.csvreader.{CsvReader, CsvWriter}

/**
  * 读取分析结果txt文件并输出到csv文件
  */
object CSVUtil {

  val header1: Array[String] = Array[String]("name", "real num", "exp num", "precision", "recall", "f-score")
  val header2: Array[String] = Array[String]("name", "exp num", "precision", "recall", "f-score")
  val header3: Array[String] = Array[String]("name", "precision", "recall", "f-score")

  /**
    * 读取指定路径下的csv文件
    *
    * @param path 文件路径
    */
  def read(path: String, columns: Array[String]): Unit = {

    // 创建CSV读对象
    val csvReader = new CsvReader(path)
    // 读表头
    csvReader.readHeaders()
    while (csvReader.readRecord()) {
      // 读一整行
      println(csvReader.getRawRecord)
      // 根据列名读列数据
      for (column <- columns) {
        println(csvReader.get(column))
      }

    }
  }


  /**
    * 读取指定路径下的csv文件
    *
    * @param path    文件路径
    * @param header  表头
    * @param records 记录数组
    */
  def write(path: String, header: Array[String] = header2, records: List[Array[String]]) {

    // 创建CSV写对象 逗号作为分隔符
    val csvWriter = new CsvWriter(path, ',', Charset.forName("GBK"))
    //CsvWriter csvWriter = new CsvWriter(filePath);

    // 写表头
    csvWriter.writeRecord(header)
    for (record <- records) {
      csvWriter.writeRecord(record)
    }
    csvWriter.close()

  }


}
