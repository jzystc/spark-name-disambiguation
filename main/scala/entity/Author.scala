package entity

/**
  * 对应author表中的每一条记录
  */
class Author {
  //author表中每条记录的主键
  var id = 0
  //作者名字
  var name = ""
  //作者所属机构名
  var org = ""
  //作者唯一标识author_id
  var authorId = ""
  //文章id
  var paperId = ""

  /**
    * 随机删除作者id
    *
    * @param rate 保留id的比率 随机生成num 大于rate则删除id
    * @return num 随机生成的数
    */

  def deleteId(rate: Int): Int = {
    if (rate < 0 || rate > 99)
      -1
    else {
      val num = scala.util.Random.nextInt(100)
      if (num >= rate) {
        this.authorId = ""
      }
      num
    }
  }

  /**
    * 存储作者信息到map中
    */
  def toMap: Map[String, Any] = {
    var info: Map[String, Any] = Map()
    info += ("id" -> id)
    info += ("name" -> name)
    info += ("org" -> org)
    info += ("authorId" -> authorId)
    info
  }
}
