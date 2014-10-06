JavaWordCountSpark
==================


# H1 Description
A basic java word count for spark


###How-to

# H4 Configuration
Optionally you'll need to set your hadoop configuration with the following commmand:
    export HADOOP_CONF_DIR=/etc/hadoop/conf   # path to your hadoop configuration


####Run locally
java -cp ./target/JavaWordCount-1.0-SNAPSHOT.jar spark.examples.JavaWordCount local[1] README.md


# H4 Run locally with 8 cores
hadoop fs -put README.md
spark-submit --class spark.examples.JavaWordCount --master local[8]  ./target/JavaWordCount-1.0-SNAPSHOT.jar local[8] README.md


# H4 Run in a yarn cluster
spark-submit --class spark.examples.JavaWordCount --master yarn-cluster  --executor-memory 1G --num-executors 2 ./target/JavaWordCount-1.0-SNAPSHOT.jar yarn-cluster README.md
