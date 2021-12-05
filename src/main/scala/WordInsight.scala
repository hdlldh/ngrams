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

    val testSubject = sc.parallelize(Seq(args(2)))

    val topNTokens = spark.read.load(args(0))
    val vocabSet = (topNTokens
      .select("token")
      .rdd
      .map(r => r.getString(0))
      .collect ++ Array(Config.StartToken, Config.EndToken, ".", "?", "!", "$")).toSet

    val probFrame = spark.read
      .load(args(1))

    val tokenizedSubject = Utils.tokenizer(testSubject)
    val extendSubject = Utils.addPrefixAndSuffix(tokenizedSubject, Config.NumStartTokens, Config.NumEndTokens)
    val trigramCount = Utils
      .countNGrams(Config.N, extendSubject)
      .withColumn("orig_word", regexp_extract($"ngram", Config.WordExtractPattern, Config.CenterWordIndex))
      .select("orig_word", "ngram")
      .rdd
      .map(r =>
        (
          r.getString(0),
          r.getString(1),
          r.getString(1)
            .split("[\\W]+")
            .map { w =>
              if (vocabSet.contains(w)) w
              else Config.UnknownToken
            }
            .mkString(" ")
        )
      )
      .toDF("orig_word", "orig_ngram", "replaced_ngram")
      .withColumn(
        "masked_ngram",
        regexp_replace($"replaced_ngram", Config.WordExtractPattern, Config.WordReplacement)
      )

    val wordHints = trigramCount
      .join(probFrame, Seq("masked_ngram"), "left")
      .na
      .fill("n/a", Seq("hint_words"))
      .select("orig_ngram", "masked_ngram", "orig_word", "hint_words")
    wordHints.show(false)

  }
}
