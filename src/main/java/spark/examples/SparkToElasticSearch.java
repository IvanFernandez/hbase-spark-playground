package spark.examples;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

import scala.Tuple2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
spark-submit --class spark.examples.SparkToElasticSearch --master yarn-cluster --executor-memory 400m --num-executors 1 ./target/hbase-spark-playground-1.0-SNAPSHOT.jar yarn-cluster
 * @author root
 *
 */
public class SparkToElasticSearch {
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		String mode = args[0];
		//mode = "local";
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("es.nodes", "192.168.149.135");
		sparkConf.setAppName("Spark to ElasticSearch PoC");
		sparkConf.setMaster(mode);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

//		
//		Map<String, String> randomValues = new HashMap();
//		randomValues.put("key", "value");
//		for(Integer i = 0; i < 100; i++) {
//			randomValues.put(i.toString(), i.toString());
//		}
//		Map<String, String> unmodifiableMap = Collections.unmodifiableMap(randomValues);
		
		Date date = new Date();
		Timestamp timestamp = new Timestamp(date.getTime());
		Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2, "timestamp", timestamp);
		Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Francisco","timestamp", timestamp);

		
		//JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers,airports,unmodifiableMap));
		JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers,airports));

		//JavaRDD<String> lines = sc.textFile("test.json");
//		JavaEsSpark.saveToEs(javaRDD, "spark/docsWithTime");
//		JavaEsSpark.saveToEs(lines, "spark/lines");
		PairFunction<Map<String, ?>, Object, Object> f = new PairFunction<Map<String, ?>, Object, Object>() {

			@Override
			public Tuple2<Object, Object> call(Map<String, ?> t)
					throws Exception {
				MapWritable object = new MapWritable();
				object.put(new Text("1234"), new Text("a value"));
				Tuple2<Object,Object> tuple = new Tuple2<Object, Object>(new Text("id"),object);
				return tuple;
			}
			
		};
		JavaPairRDD<Object, Object> mapToPair = javaRDD.mapToPair(f );
		JobConf jobConf = new JobConf(sc.hadoopConfiguration());
		jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat");
		jobConf.setOutputCommitter(FileOutputCommitter.class);
		jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, "test/hadoopDataset");
		jobConf.set("es.nodes", "192.168.149.135");
		mapToPair.saveAsHadoopDataset(jobConf );

		
		//System.exit(0);
		sc.stop();
		
	}

}
