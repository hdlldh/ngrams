import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NGramBuilder {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val vocabSize = args(0).toInt
    val corpus = sc.textFile(args(1))

    val tokenizedCorpus = Utils.tokenizer(corpus)

    val topNTokens = tokenizedCorpus
      .flatMap(_.toSeq)
      .map(r => (r, 1L))
      .reduceByKey(_ + _)
      .filter(r => r._1.matches("[a-zA-Z\\.\\?\\!]+"))
      .sortBy(_._2, false)
      .toDF("token", "count")
      .limit(vocabSize)

    topNTokens
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(2))

    val vocabSet = topNTokens
      .select("token")
      .rdd
      .map(r => r.getString(0))
      .collect
      .toSet

    val replacedCorpus = Utils.handleOov(tokenizedCorpus, vocabSet)

    val extendCorpus =
      Utils.addPrefixAndSuffix(replacedCorpus, Config.NumStartTokens, Config.NumEndTokens)
    val trigramFrame = Utils
      .countNGrams(Config.N, extendCorpus)
      .withColumn(
        "masked_ngram",
        regexp_replace($"ngram", Config.WordExtractPattern, Config.WordReplacement)
      )
      .withColumn(
        "word",
        regexp_extract($"ngram", Config.WordExtractPattern, Config.CenterWordIndex)
      )
      .withColumnRenamed("count", "numerator")

    val bigramFrame = trigramFrame
      .groupBy("masked_ngram")
      .agg(sum($"numerator").as("denominator"))

    val probFrame = trigramFrame
      .join(bigramFrame, Seq("masked_ngram"), "left")
      .na
      .fill(0L, Seq("numerator", "denominator"))
      .withColumn(
        "probability",
        ($"numerator" + lit(Config.K)) / ($"denominator" + lit(Config.K * vocabSize))
      )

    val topHintFrame = probFrame
      .filter($"word" =!= Config.UnknownToken)
      .filter($"word" =!= Config.StartToken)
      .filter($"word" =!= Config.EndToken)
      .sort($"masked_ngram", desc("probability"))
      .select("masked_ngram", "word", "probability")
      .rdd
      .map { r =>
        (r.getString(0), s"${r.getString(1)}:${r.getDouble(2).toString}")
      }
      .groupByKey()
      .mapValues(r => r.take(Config.NumHints).mkString(", "))
      .toDF("masked_ngram", "hint_words")

    topHintFrame
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(3))

  }

}
