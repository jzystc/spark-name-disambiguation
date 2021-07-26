package example

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

object LogisticRegressionExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      //若在本地运行需要设置为local
      .master("local[*]")
      //.config("spark.executor.memory", "12g")
      .getOrCreate()
    // Load training data
    val training = spark.read.format("libsvm").load("d:/programfiles/spark-2.3.4/data/mllib/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training).setThreshold(0.9)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    val df=lrModel.transform(training)
    df.show(10)
//    // We can also use the multinomial family for binary classification
//    val mlr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)
//      .setFamily("multinomial")
//
//    val mlrModel = mlr.fit(training)
//
//    // Print the coefficients and intercepts for logistic regression with multinomial family
//    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
//    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
  }

}
