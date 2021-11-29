Workspace=$HOME/Workspace/wiki
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--deploy-mode client \
--class NGramBuilder \
--name NGramBuilderApp \
--driver-memory 3G \
--driver-cores 1 \
--executor-memory 12G \
$HOME/Workspace/ngrams/target/scala-2.12/ngrams_2.12-0.1.jar \
60000 \
$Workspace/Workspace/wiki/wiki_sentences_20201201.dat.gz \
$Workspace/tmp/top_n_vocab \
$Workspace/tmp/bigram_counts \
$Workspace/tmp/trigram_counts

$SPARK_HOME/bin/spark-submit \
--master local[*] \
--deploy-mode client \
--class ProbEstimator \
--name ProbEstimatorApp \
--driver-memory 3G \
--driver-cores 1 \
--executor-memory 12G \
$HOME/Workspace/ngrams/target/scala-2.12/ngrams_2.12-0.1.jar \
$Workspace/tmp/top_n_vocab \
$Workspace/tmp/bigram_counts \
$Workspace/tmp/trigram_counts \
$Workspace/tmp/ksmooth_prob \
$Workspace/tmp/top_hint_words
