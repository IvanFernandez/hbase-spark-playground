package spark.examples;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * 
 * @author root 
 *         spark-submit --class
 *         spark.examples.SparkToElasticSearchStreaming --master yarn-cluster
 *         --executor-memory 400m --num-executors 1
 *         ./target/hbase-spark-playground-1.0-SNAPSHOT.jar yarn-cluster
 *         doc2yarn 
    java -cp ./target/hbase-spark-playground-1.0-SNAPSHOT.jar spark.examples.SparkToElasticSearchStreaming local[2] collection-name
 */
public class SparkToElasticSearchStreaming {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SparkToElasticSearchStreaming.class);

	public static void main(String[] args) {
		String mode = args[0];
		final String collection = args[1];
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("es.nodes", "192.168.149.135");
		sparkConf.setAppName("Spark to ElasticSearch PoC");
		sparkConf.setMaster(mode);

		// Twitter configuration
		// http://stackoverflow.com/questions/20638190/twitter4j-authentication-credentials-are-missing
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey("KdR2xuTfAzRcMFnA9mTuA")
				.setOAuthConsumerSecret(
						"Kf6EawZE9qTWu3QfpSzye96cEEFln4mm6tHdvkwKk")
				.setOAuthAccessToken(
						"252032085-CyUirikxiZNRtcTJQ9qJUosxY1AaMSyyD5wzvGHz")
				.setOAuthAccessTokenSecret(
						"WcSV3yJjN42izwCoyVxsYzpH18tgtEPZ44v2Q054QQE");

		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				new Duration(1000));

		JavaDStream<Status> tweets = TwitterUtils.createStream(jssc,
				twitter.getAuthorization());
		JavaDStream<Map<String, String>> statuses = tweets
				.map(new Function<Status, Map<String, String>>() {
					public Map<String, String> call(Status status) {
						Map<String, String> tweet = new HashMap<String, String>();
						Date date = new Date();
						Timestamp timestamp = new Timestamp(date.getTime());
						SimpleDateFormat dt1 = new SimpleDateFormat(
								"yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
						tweet.put("timestamp", dt1.format(date));
						tweet.put("created_at", status.getCreatedAt() + "");
						tweet.put("user", status.getUser().getName());
						LOGGER.info("--> Inside Mapper: " + status.toString());
						return tweet;
						// return status.getUser().getName();
					}
				});

		statuses.print();
		
		// usando saveAsHadoopDataset
//		statuses.foreach(new Function<JavaRDD<Map<String, String>>, Void>() {
//			private static final long serialVersionUID = 6272424972267329328L;
//			@Override
//			public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
//				JavaPairRDD<Object, Object> mapToPair = rdd.mapToPair(new PairFunction<Map<String, String>, Object, Object>() {
//
//					@Override
//					public Tuple2<Object, Object> call(Map<String, String> t)
//							throws Exception {
//						MapWritable object = new MapWritable();
//						object.put(new Text("test"), new Text("test value"));
//						Tuple2<Object,Object> tuple = new Tuple2<Object, Object>(new Text("id"),object);
//						return tuple;
//					}
//					
//				});
//				JobConf jobConf = new JobConf();
//				jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat");
//				jobConf.setOutputCommitter(FileOutputCommitter.class);
//				jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, "test/" + collection);
//				jobConf.set("es.nodes", "192.168.149.135");
//				FileOutputFormat.setOutputPath(jobConf, new Path("-"));
//				mapToPair.saveAsHadoopDataset(jobConf);
//				return (Void) null;
//			}
//		});

		// usando JavaEsSpark.saveToEs
		statuses.foreach(new Function<JavaRDD<Map<String, String>>, Void>() {
			private static final long serialVersionUID = 6272424972267329328L;
			@Override
			public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
				//con configuracion
				Map<String,String> conf = new HashMap<String,String>();
				conf.put("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat");
				conf.put(ConfigurationOptions.ES_RESOURCE_WRITE, "test/hadoopDatasetSaveToEs");
				conf.put("es.nodes", "192.168.149.135");
				JavaEsSpark.saveToEs(rdd, "test/" + collection);
				//sin configuracion
				//JavaEsSpark.saveToEs(rdd, "test/" + collection);
				
				return (Void) null;
			}
		});

		//statuses.print();
		jssc.start();
		jssc.awaitTermination(10000);
	}
}
