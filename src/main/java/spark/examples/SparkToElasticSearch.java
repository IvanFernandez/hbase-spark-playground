package spark.examples;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SparkToElasticSearch {
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		String mode = args[0];
		mode = "local";
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

		JavaEsSpark.saveToEs(javaRDD, "spark/docsWithTime");
		
		sc.stop();
		
	}

}
