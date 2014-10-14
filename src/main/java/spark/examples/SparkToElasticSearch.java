package spark.examples;

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
		Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
		Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Francisco");
		
		JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers,airports));
		JavaEsSpark.saveToEs(javaRDD, "spark/docs");
		
		sc.stop();
		
	}

}
