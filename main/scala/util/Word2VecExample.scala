/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package util

// $example on$
import org.apache.spark.ml.feature.{Tokenizer, Word2Vec}
// $example off$
import org.apache.spark.sql.SparkSession

object Word2VecExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Word2Vec example")
      .getOrCreate()
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    val doc = spark.createDataFrame(Seq(
      "Beijing University",
      "Beijing technical University",
      ""
    ).map(Tuple1.apply)).toDF("text")

    val a = tokenizer.transform(doc)
    val d = tokenizer.transform(doc.filter("text!=\"\""))
    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model1 = word2Vec.fit(a)
    val model2 = word2Vec.fit(d)

    val df1 = model1.transform(a)
    val df2 = model2.transform(d)
    df1.show()
    df2.show()
    spark.stop()
  }
}

// scalastyle:on println