JAR_FILE=$HOME/IdeaProjects/ngrams/target/scala-2.12/ngrams_2.12-0.1.jar
VOCAB_SIZE=60000
INPUT_DATA=$HOME/PycharmProjects/wiki_scala/wiki_sentences_samples.dat.gz
OUTPUT_FOLDER=$HOME/Workspace/ngrams/tmp
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--deploy-mode client \
--class NGramBuilder \
--name NGramBuilderApp \
--driver-memory 3G \
--driver-cores 1 \
--executor-memory 12G \
$JAR_FILE \
$VOCAB_SIZE \
$INPUT_DATA \
$OUTPUT_FOLDER/top_n_vocab \
$OUTPUT_FOLDER/top_hint_words
