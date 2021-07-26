package util

/**
 * paths to some necessary files
 */
object Settings {
  val dataset = "kdd"
  val hdfsPrefix = "hdfs://datacsu1:9000/"
  val testJsonPath = s"/root/$dataset/name_test_100.json"
  val trainJsonPath = s"/root/$dataset/name_train_500.json"
  val vidPidJsonPath = s"/root/$dataset/vidpid/"
  //srcid dstid 概率值
  val simSavePath = s"/root/$dataset/sim/"
  //word2vec模型位置
  val w2vModelPath = s"$hdfsPrefix/word2vec/word2vec_100"
  //验证集pubjson位置
  val pubsJsonPath = s"/root/$dataset/pubs_block_org_name_0.80.json"
  val venuesJsonPath = s"$hdfsPrefix/$dataset/clean_venues.json"
  //单个作者的libsvm文件保存位置
  val libsvmSavePath = s"/root/$dataset/libsvm"
  //venue分类json保存位置
  //lr模型保存位置
  val modelSavePath = s"$hdfsPrefix/$dataset/rf"
  val trainingDataPath = s"$hdfsPrefix/$dataset/samples"
  //合并后的libsvm.txt在本地中存放的位置
  val unionLibsvmPath = s"/root/$dataset/libsvm.txt"
  //合并后的libsvm.txt在hdfs中存放的位置
  val unionLibsvmHdfsPath = s"$hdfsPrefix/$dataset/libsvm.txt"
  //聚类后的训练数据
  val clusteredDataPath = s"$hdfsPrefix/$dataset/clustered_data"
  val sampledDataPath = s"$hdfsPrefix/$dataset/samples"
  val resultSavePath = s"/root/$dataset/result2"
  val singleResultSavePath = s"d:/sigir2020/$dataset/single"
  val modelPath = s"$hdfsPrefix/$dataset/lr"
  //  val modelPath = "/user/root/contest/svm"
  //  val modelPath = "/user/root/contest/lr_noorg"
  val thresholdForAbbrName = 0.5 //针对缩写名字的阈值
  val thresholdForNormalName = 0.5 //针对不缩写名字的阈值
}
