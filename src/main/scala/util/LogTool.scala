package util

import org.apache.log4j.Logger

object LogTool {
  val logger: Logger = Logger.getLogger(this.getClass)

  def log(log: String) {
    // System.out.println("This is println message.");
    // 记录error级别的信息
    logger.error(log)
  }
}
