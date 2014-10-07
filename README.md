JavaWordCountSpark
==================


###Description
- A basic java word count for spark	@see spark.examples.JavaWordCount
- A basic java spark to hbase write @see spark.examples.SparkToHBaseWriter


####How-to
Optionally you'll need to set your hadoop configuration with the following commmand:
    export HADOOP_CONF_DIR=/etc/hadoop/conf   # path to your hadoop configuration


####Run locally
- Word count
java -cp ./target/JavaWordCount-1.0-SNAPSHOT.jar spark.examples.JavaWordCount local[1] README.md
- HBase reader
java -cp ./target/JavaWordCount-1.0-SNAPSHOT.jar spark.examples.SparkToHBaseWriter local[1]

####Run locally with 8 cores
hadoop fs -put README.md
spark-submit --class spark.examples.JavaWordCount --master local[8]  ./target/JavaWordCount-1.0-SNAPSHOT.jar local[8] README.md
spark-submit --class spark.examples.SparkToHBaseWriter --master local[8]  ./target/JavaWordCount-1.0-SNAPSHOT.jar local[8]



####Run in a yarn cluster
spark-submit --class spark.examples.JavaWordCount --master yarn-cluster  --executor-memory 1G --num-executors 2 ./target/JavaWordCount-1.0-SNAPSHOT.jar yarn-cluster README.md
spark-submit --class spark.examples.SparkToHBaseWriter --master yarn-cluster  --executor-memory 1G --num-executors 2 ./target/JavaWordCount-1.0-SNAPSHOT.jar yarn-cluster

