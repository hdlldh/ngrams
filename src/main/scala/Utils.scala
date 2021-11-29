import org.apache.spark.ml.feature.NGram
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def tokenizer(corpus: RDD[String]): RDD[Array[String]] = {
    corpus
      .map(r => r.toLowerCase)
      .map(r => r.replaceAll("[^\\w\\.\\?\\! ]+", " "))
      .map(r => r.replaceAll("\\.", " ."))
      .map(r => r.replaceAll("\\?", " ?"))
      .map(r => r.replaceAll("\\!", " !"))
      .map(r => r.split("\\s+"))
      .filter(r => r.length >= NGramConfig.MinNumTokens)
  }

  def handleOov(tokenizedCorpus: RDD[Array[String]], vocabSet: Set[String]): RDD[Array[String]] = {
    tokenizedCorpus
      .map(r =>
        r.map { w =>
          if (vocabSet.contains(w)) w
          else NGramConfig.UnknownToken
        }
      )
  }

  def addPrefixAndSuffix(
    replacedCorpus: RDD[Array[String]],
    numStartToken: Int,
    numEndToken: Int
  ): RDD[Array[String]] = {
    val prefix = (1 to numStartToken).map(_ => NGramConfig.StartToken).toArray
    val suffix = (1 to numEndToken).map(_ => NGramConfig.EndToken).toArray
    replacedCorpus.map(r => prefix ++ r ++ suffix)
  }

  def countNGrams(
    n: Int,
    extCorpus: RDD[Array[String]]
  )(implicit
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    val inputCol = "sentence"
    val outputCol = "ngrams"

    val ngram = new NGram()
      .setN(n)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)

    ngram
      .transform(extCorpus.toDF(inputCol))
      .select(outputCol)
      .rdd
      .map(r => r.getSeq[String](0))
      .flatMap(_.toSeq)
      .map(r => (r, 1L))
      .reduceByKey(_ + _)
      .toDF("ngram", "count")
  }
}
