import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

object ProbEstimator {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val topNTokens = spark.read.load(args(0))
    val vocabSize = topNTokens.count()
    println(vocabSize)

    val bigramFrame = spark.read
      .load(args(1))
      .withColumnRenamed("ngram", "masked_ngram")
      .withColumnRenamed("count", "denominator")

    val trigramFrame = spark.read
      .load(args(2))
      .withColumnRenamed("count", "numerator")
      .withColumn("word", regexp_extract($"ngram", "(\\S+) (\\S+) (\\S+)", 2))
      .withColumn(
        "masked_ngram",
        regexp_replace($"ngram", NGramConfig.WordPattern, NGramConfig.WordReplacement)
      )

    val probFrame = trigramFrame
      .join(bigramFrame, Seq("masked_ngram"), "left")
      .na
      .fill(0L, Seq("numerator", "denominator"))
      .withColumn(
        "probability",
        ($"numerator" + lit(NGramConfig.K)) / ($"denominator" + lit(NGramConfig.K + vocabSize + 2))
      )

    probFrame
      .select("masked_ngram", "word", "probability")
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(3))

    val topHintFrame = probFrame
      .filter($"word" =!= NGramConfig.UnknownToken)
      .filter($"word" =!= NGramConfig.StartToken)
      .filter($"word" =!= NGramConfig.EndToken)
      .sort($"masked_ngram", desc("probability"))
      .select("masked_ngram", "word", "probability")
      .rdd
      .map { r =>
        (r.getString(0), s"${r.getString(1)}:${r.getDouble(2).toString}")
      }
      .groupByKey()
      .mapValues(r => r.take(NGramConfig.NumHints).mkString(", "))
      .toDF("masked_ngram", "hint_words")

    topHintFrame
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(4))
  }
}
