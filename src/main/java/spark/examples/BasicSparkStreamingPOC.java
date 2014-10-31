package spark.examples;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class BasicSparkStreamingPOC {

	private static final Logger LOGGER = LoggerFactory.getLogger(BasicSparkStreamingPOC.class);
	
	public static void main(String[] args) {
		String master = args[0];
		String hdfsUrl = args[1];
		JavaStreamingContext jssc = new JavaStreamingContext(master,
				BasicSparkStreamingPOC.class.getSimpleName(), new Duration(10000));
		//jssc.checkpoint(hdfsUrl + "checkpoint");
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
				"localhost", 9999);

		JavaDStream<String> words = lines.flatMap(tokenizerFunction());
		JavaPairDStream<String, Integer> tuples = words
				.mapToPair(emitterWordAppeareanceFunction());
		JavaPairDStream<String, Integer> wordCounts = tuples
				.reduceByKey(wordCounterReducerFunction());

		wordCounts.print();
		//wordCounts.checkpoint(new Duration(30000));
		@SuppressWarnings("unchecked")
		Class<? extends OutputFormat<?,?>> outputFormatClass = (Class<? extends OutputFormat<?,?>>) (Class<?>) SequenceFileOutputFormat.class;
		wordCounts.saveAsNewAPIHadoopFiles(hdfsUrl, "/word-count-old",Text.class, Text.class, outputFormatClass);
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate

	}

	private static FlatMapFunction<String, String> tokenizerFunction() {
		return new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 2622638376941957935L;

			@Override
			public Iterable<String> call(String sentence) throws Exception {
				return Arrays.asList(sentence.split(" "));
			}

		};
	}

	private static PairFunction<String, String, Integer> emitterWordAppeareanceFunction() {
		return new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = -5470217640118788770L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		};
	}

	private static Function2<Integer, Integer, Integer> wordCounterReducerFunction() {
		return new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = -4623555865421779573L;
			@Override
			public Integer call(Integer i1, Integer i2) throws Exception {
				return i1 + i2;
			}
		};
	}

}
