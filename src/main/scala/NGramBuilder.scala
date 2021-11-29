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

    val extendCorpus = Utils.addPrefixAndSuffix(replacedCorpus, 1, 1)
    val trigramFrame = Utils.countNGrams(3, extendCorpus)
    trigramFrame
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(4))

    val bigramFrame = trigramFrame
      .withColumn(
        "masked_ngram",
        regexp_replace($"ngram", NGramConfig.WordPattern, NGramConfig.WordReplacement)
      )
      .groupBy("masked_ngram")
      .agg(sum($"count").as("count"))

    bigramFrame
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(3))

  }

}
