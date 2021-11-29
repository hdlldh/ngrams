import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, regexp_extract, regexp_replace}

object WordInsight {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

//    val testSubject = sc.parallelize(Seq("Get Netflix for an entire year for $39.99 only."))
    val testSubject = sc.parallelize(Seq(args(2)))

    val topNTokens = spark.read.load(args(0))
    val vocabSet = topNTokens
      .select("token")
      .rdd
      .map(r => r.getString(0))
      .collect
      .toSet

    val probFrame = spark.read
      .load(args(1))
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

    val tokenizedSubject = Utils.tokenizer(testSubject)
    val replacedSubject = Utils.handleOov(tokenizedSubject, vocabSet)
    val extendSubject = Utils.addPrefixAndSuffix(replacedSubject, 1, 1)
    val trigramCount = Utils
      .countNGrams(3, extendSubject)
      .withColumn("orig_word", regexp_extract($"ngram", "(\\S+) (\\S+) (\\S+)", 2))
      .withColumn(
        "masked_ngram",
        regexp_replace($"ngram", NGramConfig.WordPattern, NGramConfig.WordReplacement)
      )

    val wordHints = trigramCount
      .join(probFrame, Seq("masked_ngram"), "left")
      .na
      .fill("n/a", Seq("hint_words"))
      .select("masked_ngram", "orig_word", "hint_words")
    wordHints.show(false)

  }
}