hbase-spark-playgroundSpark
==================


###Description
- A basic Java class that reads and writes in HBase @see spark.examples.HBaseUtils
- A basic java word count for spark	@see spark.examples.SparkWordCount
- A basic java class that uses spark for reading and writing in HBase @see spark.examples.SparkToHBase


####How-to
Optionally you'll need to set your hadoop configuration with the following commmand:
    export HADOOP_CONF_DIR=/etc/hadoop/conf   # path to your hadoop configuration


####Run locally
- Word count
java -cp ./target/hbase-spark-playground-1.0-SNAPSHOT.jar spark.examples.SparkWordCount local[1] README.md
- HBase reader
java -cp ./target/hbase-spark-playground-1.0-SNAPSHOT.jar spark.examples.SparkToHBase local[1]

####Run locally with 8 cores
hadoop fs -put README.md
spark-submit --class spark.examples.SparkWordCount --master local[8]  ./target/hbase-spark-playground-1.0-SNAPSHOT.jar local[8] README.md
spark-submit --class spark.examples.SparkToHBase --master local[8]  ./target/hbase-spark-playground-1.0-SNAPSHOT.jar local[8]



####Run in a yarn cluster
spark-submit --class spark.examples.SparkWordCount --master yarn-cluster  --executor-memory 1G --num-executors 2 ./target/hbase-spark-playground-1.0-SNAPSHOT.jar yarn-cluster README.md
spark-submit --class spark.examples.SparkToHBase --master yarn-cluster  --executor-memory 1G --num-executors 2 ./target/hbase-spark-playground-1.0-SNAPSHOT.jar yarn-cluster

