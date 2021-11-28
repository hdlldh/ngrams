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

  def oovHandler(tokenizedCorpus: RDD[Array[String]], vocabSet: Set[String]): RDD[Array[String]] = {
    tokenizedCorpus
      .map(r =>
        r.map { w =>
          if (vocabSet.contains(w)) w
          else NGramConfig.UnknownToken
        }
      )
  }

  def tokenAdder(n: Int, replacedCorpus: RDD[Array[String]]): RDD[Array[String]] = {
    val range = 1 until n
    val prefix = range.map(_ => NGramConfig.StartToken).toArray
    val suffix = range.map(_ => NGramConfig.EndToken).toArray
    replacedCorpus.map(r => prefix ++ r ++ suffix)
  }

  def ngramCounter(
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
