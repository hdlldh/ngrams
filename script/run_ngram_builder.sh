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
$HOME/Workspace/wiki/wiki_sentences_20201201.dat.gz \
$HOME/tmp/top_n_vocab \
$HOME/tmp/bigram_counts \
$HOME/tmp/trigram_counts
