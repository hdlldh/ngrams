import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    val tokenizedSubject = Utils.tokenizer(testSubject)
//    val replacedSubject = Utils.handleOov(tokenizedSubject, vocabSet)
    val extendSubject = Utils.addPrefixAndSuffix(tokenizedSubject, 1, 1)
    val trigramCount = Utils
      .countNGrams(3, extendSubject)
      .withColumn("orig_word", regexp_extract($"ngram", "(\\S+) (\\S+) (\\S+)", 2))
      .select("orig_word", "ngram")
      .rdd
      .map(r =>
        (
          r.getString(0),
          r.getString(1),
          r.getString(1)
            .split("\\s+")
            .map { w =>
              if (vocabSet.contains(w)) w
              else NGramConfig.UnknownToken
            }
            .mkString(" ")
        )
      )
      .toDF("orig_word", "orig_ngram", "replaced_ngram")
      .withColumn(
        "masked_ngram",
        regexp_replace($"replaced_ngram", NGramConfig.WordPattern, NGramConfig.WordReplacement)
      )

    val wordHints = trigramCount
      .join(probFrame, Seq("masked_ngram"), "left")
      .na
      .fill("n/a", Seq("hint_words"))
      .select("orig_ngram","masked_ngram", "orig_word", "hint_words")
    wordHints.show(false)

  }
}
