import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, split}

object WordInsight {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

//    val testSubject = sc.parallelize(Seq("Get Netflix for an entire year for $39.99 only."))
    val testSubject = sc.parallelize(Seq(args(3)))

    val topNTokens = spark.read.load(args(0))
//    val vocabSize = topNTokens.count()
    val vocabSet = topNTokens
      .select("token")
      .rdd
      .map(r => r.getString(0))
      .collect
      .toSet

    val forwardProb = spark.read
      .load(args(1))
      .sort($"prevBigram", desc("probability"))
      .rdd
      .map(r => (r.getString(0), s"${r.getString(1)}:${r.getDouble(2).toString}"))
      .groupByKey()
      .mapValues(r => r.take(NGramConfig.NumHints).mkString(", "))
      .toDF("prevBigram", "wordHint")

    val backwardProb = spark.read
      .load(args(2))
      .sort($"nextBigram", desc("probability"))
      .rdd
      .map(r => (r.getString(1), s"${r.getString(0)}:${r.getDouble(2).toString}"))
      .groupByKey()
      .mapValues(r => r.take(NGramConfig.NumHints).mkString(", "))
      .toDF("nextBigram", "wordHint")

    val tokenizedSubject = Utils.tokenizer(testSubject)
    val replacedSubject = Utils.oovHandler(tokenizedSubject, vocabSet)
    val extendSubject = Utils.tokenAdder(2, replacedSubject)
    val trigramCount = Utils
      .ngramCounter(3, extendSubject)
      .withColumn("trigramArray", split($"ngram", " "))

    val forwardFrame = trigramCount
      .select("trigramArray")
      .rdd
      .map(r => (r.getSeq[String](0).dropRight(1).mkString(" "), r.getSeq[String](0).last))
      .toDF("prevBigram", "nextOrigWord")
      .filter($"nextOrigWord" =!= NGramConfig.UnknownToken)
      .filter($"nextOrigWord" =!= NGramConfig.EndToken)

    val wordForward = forwardFrame
      .join(forwardProb, Seq("prevBigram"), "left")
      .select("prevBigram", "nextOrigWord", "wordHint")
    wordForward.show(false)

    val backwardFrame = trigramCount
      .select("trigramArray")
      .rdd
      .map(r => (r.getSeq[String](0).head, r.getSeq[String](0).drop(1).mkString(" ")))
      .toDF("prevOrigWord", "nextBigram")
      .filter($"prevOrigWord" =!= NGramConfig.UnknownToken)
      .filter($"prevOrigWord" =!= NGramConfig.StartToken)

    val wordBackward = backwardFrame
      .join(backwardProb, Seq("nextBigram"), "left")
      .select("prevOrigWord", "nextBigram", "wordHint")
    wordBackward.show(false)
  }
}
