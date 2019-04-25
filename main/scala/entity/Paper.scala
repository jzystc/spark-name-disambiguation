package entity

/**
  * 对应paper表中每一条记录
  */
class Paper {
  //标题
  var title = ""
  //发表年份
  var year: Int = 0
  //文章所属期刊或会议名
  var venue = ""
  //文章id
  var paperId = ""
  //文章摘要
  var pAbstract = ""

  def toMap: Map[String, Any] = {
    var info: Map[String, Any] = Map()
    info += ("title" -> title)
    info += ("year" -> year)
    info += ("venue" -> venue)
    info += ("paperId" -> paperId)
    info += ("pAbstract" -> pAbstract)
    info
  }
}
