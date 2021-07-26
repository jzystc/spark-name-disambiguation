package util

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FileUtil {
  val DEFAULT_FS = "hdfs://datacsu1:9000/"

  def deleteDirectory(filePath: String): Unit = {
    val file = new File(filePath)
    if (!file.exists()) {
      return
    }
    if (file.isFile) {
      file.delete()
    } else if (file.isDirectory) {
      val files = file.listFiles()
      for (myfile <- files) {
        deleteDirectory(filePath + "/" + myfile.getName)
      }
      file.delete()
    }
  }

  def upload2hdfs(localPath: String, hdfsPath: String): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", DEFAULT_FS)
    try {
      val src = new Path(localPath)
      val dst = new Path(hdfsPath)
      val hdfs = FileSystem.get(conf)
      if (!hdfs.exists(dst)) {
        hdfs.mkdirs(dst)
      }
      hdfs.copyFromLocalFile(src, dst)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
