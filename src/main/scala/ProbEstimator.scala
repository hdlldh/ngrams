import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
      .withColumnRenamed("count", "denominator")

    val trigramFrame = spark.read
      .load(args(2))
      .withColumnRenamed("count", "numerator")
      .withColumn("trigramArray", split($"ngram", " "))
      .filter(size($"trigramArray") === 3)

    val forwardTrigram = trigramFrame
      .select("numerator", "trigramArray")
      .rdd
      .map(r =>
        (r.getLong(0), r.getSeq[String](1).dropRight(1).mkString(" "), r.getSeq[String](1).last)
      )
      .toDF("numerator", "prevBigram", "nextWord")
      .filter($"nextWord" =!= NGramConfig.UnknownToken)
      .filter($"nextWord" =!= NGramConfig.EndToken)

    val forwardProb = forwardTrigram
      .join(bigramFrame, forwardTrigram("prevBigram") === bigramFrame("ngram"), "left")
      .withColumn(
        "probability",
        ($"numerator" + lit(NGramConfig.K)) / ($"denominator" + lit(NGramConfig.K + vocabSize + 2))
      )
      .select("prevBigram", "nextWord", "probability")

    forwardProb
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(3))

    val backwardTrigram = trigramFrame
      .select("numerator", "trigramArray")
      .rdd
      .map(r => (r.getLong(0), r.getSeq[String](1).head, r.getSeq[String](1).drop(1).mkString(" ")))
      .toDF("numerator", "prevWord", "nextBigram")
      .filter($"prevWord" =!= NGramConfig.UnknownToken)
      .filter($"prevWord" =!= NGramConfig.StartToken)

    val backwardProb = backwardTrigram
      .join(bigramFrame, backwardTrigram("nextBigram") === bigramFrame("ngram"), "left")
      .withColumn(
        "probability",
        ($"numerator" + lit(NGramConfig.K)) / ($"denominator" + lit(NGramConfig.K + vocabSize + 2))
      )
      .select("prevWord", "nextBigram", "probability")

    backwardProb
      .repartition(1)
      .write
      .mode("overwrite")
      .save(args(4))
  }
}
