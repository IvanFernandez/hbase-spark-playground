package spark.examples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicSparkBatchPOC {
	private static final Logger LOGGER = LoggerFactory.getLogger(BasicSparkBatchPOC.class);
	
	public static void main(String[] args) {
		String master = args[0];
		String hdfsUrl = args[1];
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster(master);
		sparkConf.setAppName(BasicSparkBatchPOC.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile("pom.xml");
		//JavaRDD<String> lines = sc.textFile("hdfs://192.168.149.135:8020/user/root/README.md");

		List<String> linesCollected = lines.collect();
		LOGGER.info(linesCollected.toString());

		
		VoidFunction<String> f = new VoidFunction<String>() {

			@Override
			public void call(String t) throws Exception {
				Map<String, String> hash = new HashMap<String, String>();
				
				
			}
			
		};
		lines.foreach(f );
		
		
		
		//lines.collect();
		//JavaRDD<String> lines = sc.textFile(new Path("/root/workspace/hbase-spark-playground/src/main/resources/twitterCredentials.properties"));
//		JavaRDD<Integer> lineLengths = lines.map(lineLengthFunction());
//		int totalLength = lineLengths.reduce(countGlobalFunction() );
//		LOGGER.debug("Total length: " + totalLength);
	}

	/**
	 * @return
	 */
	private static Function2<Integer, Integer, Integer> countGlobalFunction() {
		return new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		};
	}

	/**
	 * @return
	 */
	private static Function<String, Integer> lineLengthFunction() {
		return new Function<String, Integer>() {

			@Override
			public Integer call(String line) throws Exception {
				return line.length();
			}
			
		};
	}
}
