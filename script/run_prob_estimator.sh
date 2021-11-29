$SPARK_HOME/bin/spark-submit \
--master local[*] \
--deploy-mode client \
--class ProbEstimator \
--name ProbEstimatorApp \
--driver-memory 3G \
--driver-cores 1 \
--executor-memory 12G \
$HOME/Workspace/ngrams/target/scala-2.12/ngrams_2.12-0.1.jar \
$HOME/tmp/top_n_vocab \
$HOME/tmp/bigram_counts \
$HOME/tmp/trigram_counts \
$HOME/tmp/ksmooth_prob