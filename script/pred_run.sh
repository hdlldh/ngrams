JAR_FILE=$HOME/Workspace/ngrams/target/scala-2.12/ngrams_2.12-0.1.jar
INPUT_FOLDER=$HOME/Workspace/ngrams/output/slt
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--deploy-mode client \
--class WordInsight \
--name WordInsightApp \
--driver-memory 3G \
--driver-cores 1 \
--executor-memory 12G \
$JAR_FILE \
$INPUT_FOLDER/top_n_vocab \
$INPUT_FOLDER/top_hint_words \
"Important messages regarding your checking account"
