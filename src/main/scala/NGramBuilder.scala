import org.apache.spark.sql.SparkSession

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

    val replacedCorpus = Utils.oovHandler(tokenizedCorpus, vocabSet)

    val extBiGram = Utils.tokenAdder(2, replacedCorpus)
    val bigramFrame = Utils.ngramCounter(2, extBiGram)

    bigramFrame
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(3))

    val extTriGram = Utils.tokenAdder(3, replacedCorpus)
    val trigramFrame = Utils.ngramCounter(3, extTriGram)

    trigramFrame
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(4))

  }

}
