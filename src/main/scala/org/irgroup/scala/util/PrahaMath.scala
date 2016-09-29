package org.irgroup.scala.util

import breeze.linalg.sum
import org.apache.spark.ml.linalg.Vector


/**
  * <pre>
  *      com.wemakeprice.praha.analyzer.scala.util
  * |_ PrahaMath.java
  * </pre>
  * <p>
  * <pre>
  *
  * </pre>
  *
  * @Company : (주)위메프
  * @Author : 김태준(kiora1120@wemakeprice.com)
  * @Date : 2016. 8. 8.
  * @Version : 1.0
  */
object PrahaMath {

  /**
    * Euclidean Distance
    *
    * @param vectorA A
    * @param vectorB B
    * @return 가까울수록 0에 근접
    * @see http://hyunje.com/data%20analysis/2015/10/24/advanced-analytics-with-spark-ch5/
    */
  def euclideanDistance(vectorA: Vector, vectorB: Vector) = {
    math.sqrt(vectorA.toArray.zip(vectorB.toArray).map(p => math.pow(p._1 - p._2, 2)).sum)
  }

  /**
    * Cosine Similarity
    *
    * @param vectorA A
    * @param vectorB B
    * @return −1은 서로 완전히 반대되는 경우, 0은 서로 독립적인 경우, 1은 서로 완전히 같은 경우를 의미
    * @see http://stackoverflow.com/questions/32645231/calculating-cosine-similarity-by-featurizing-the-text-into-vector-using-tf-idf
    * @see https://ko.wikipedia.org/wiki/코사인_유사도
    */
  def cosineSimilarity(vectorA: Vector, vectorB: Vector) = {

    var dotProduct = 0.0
    var normA = 0.0
    var normB = 0.0
    val index = vectorA.size - 1

    for (i <- 0 to index) {
      dotProduct += vectorA(i) * vectorB(i)
      normA += Math.pow(vectorA(i), 2)
      normB += Math.pow(vectorB(i), 2)
    }

    dotProduct / (math.sqrt(normA) * math.sqrt(normB))
  }
}
