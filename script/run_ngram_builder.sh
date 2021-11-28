$SPARK_HOME/bin/spark-submit --master local[*] \
--deploy-mode client \
--class NGramBuilder \
--name NGramBuilderOnSpark \
--driver-memory 1024M \
--driver-cores 1 \
--executor-memory 1G \
$HOME/IdeaProjects/ngrams/target/scala-2.12/ngrams_2.12-0.1.jar \
10 \
$HOME/PycharmProjects/wiki_scala/wiki_sentences_samples.dat.gz \
$HOME/tmp/top_n_vocab $HOME/tmp/bigram_counts \
$HOME/tmp/trigram_counts
