package spark.examples;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * 
 * @author root spark-submit --class
 *         spark.examples.SparkToElasticSearchStreaming --master yarn-cluster
 *         --executor-memory 200m --num-executors 1
 *         ./target/hbase-spark-playground-1.0-SNAPSHOT.jar yarn-cluster
 */
public class SparkToElasticSearchStreaming {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SparkToElasticSearchStreaming.class);

	public static void main(String[] args) {
		String mode = args[0];
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("es.nodes", "192.168.149.135");
		sparkConf.setAppName("Spark to ElasticSearch PoC");
		sparkConf.setMaster(mode);


		// Twitter configuration
		//http://stackoverflow.com/questions/20638190/twitter4j-authentication-credentials-are-missing
		ConfigurationBuilder cb = new ConfigurationBuilder();
//		cb.setDebugEnabled(true)
//				.setOAuthConsumerKey("fdeiBLYdBrgOe5lLzUqu6A")
//				.setOAuthConsumerSecret(
//						"8Pb9VF2nPbUFFW2EdG8zMDMEqF2Y1lV39QWDOAghs8")
//				.setOAuthAccessToken(
//						"2400346706-enxNX50hC4HPmiiZVQZ3vEUODyH98DVX1eB5Sc8")
//				.setOAuthAccessTokenSecret(
//						"fqgSeGGv2n9RFeJYTZcryOXrdr8CDJ8fwROjiGKZ9i0qd");
		
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

		
		JavaDStream<Status> tweets = TwitterUtils.createStream(jssc, twitter.getAuthorization());
		JavaDStream<Map<String, String>> statuses = tweets
				.map(new Function<Status, Map<String, String>>() {
					public Map<String, String> call(Status status) {
						Map<String, String> tweet = new HashMap<String, String>();
						Date date = new Date();
						Timestamp timestamp = new Timestamp(date.getTime());
						 SimpleDateFormat dt1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
						tweet.put("timestamp", dt1.format(date));
						tweet.put("created_at", status.getCreatedAt() +"");
						tweet.put("user", status.getUser().getName());
						LOGGER.info("--> Inside Mapper: " + status.toString());
						return tweet;
						//return status.getUser().getName();
					}
				});

		statuses.foreach(new Function<JavaRDD<Map<String, String>>, Void>() {

			private static final long serialVersionUID = 6272424972267329328L;

			@Override
			public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
				JavaEsSpark.saveToEs(rdd, "spark/docs3");
				List<Map<String, String>> data = rdd.collect();
				LOGGER.info("--> Inside Foreach: " + data);
				return (Void) null;
			}
		});		
		
		statuses.print();

		jssc.start();
		jssc.awaitTermination(20000);
		// sc.stop();

	}
}
// JavaDStream<Status> tweets = ssc.twitterStream();