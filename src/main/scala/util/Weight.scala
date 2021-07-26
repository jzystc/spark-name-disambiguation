package util

object Weight {

  //sim=alpha*layerSim*(beta*orgSim+(1-beta)*text)+(1-alpha)*coauthorSim

  //文章和机构相似分数之和的权重=alpha,合作者相似分数的权重=1-alpha
  var alpha: Double = 0.6

  //机构相似分数的权重=beta,文本相似分数的权重=1-beta
  var beta: Double = 0.5

  //相似分数阈值
  var threshold: Double = 0.2059432519813321

  var wTitle = 0.38563307717275275
  var wAbstract = 1.8040611828538995
  var wOrg = 0.25644516737021883
  var wCoauthor = 6.55495955809765

  //截距
  var intercept: Double = -4.297453577283057
  // 单个重名合作者的增益系数
  // var coCoefficient: Double = 0.1

}
