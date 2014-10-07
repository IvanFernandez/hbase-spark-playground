package spark.examples;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.hadoop.mapreduce.Job;

import scala.Tuple2;

//http://codereview.stackexchange.com/questions/56641/producing-a-sorted-wordcount-with-spark

public class SparkToHBaseWriter {
	
	private final static String tableName = "test";
	private final static String columnFamily = "cf";

	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("file:///etc/hbase/conf.dist/hbase-site.xml"));
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		
		//Job jobConf = Job.getInstance(conf);
		//jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		//jobConf.setOutputFormatClass(Put.class);
		
		JobConf jobConf = new JobConf(conf, SparkToHBaseWriter.class);
		jobConf.setOutputFormat(TableOutputFormat.class);
		jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		
//		jobConf.setOutputKeyClass(ImmutableBytesWritable.class); 
//		jobConf.setOutputValueClass(Put.class); 

		
		HBaseAdmin hBaseAdmin = null;
		try {
			hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.isTableAvailable(tableName)) {
				System.out.println("Table " + tableName + " is available.");
			}
			else {
				System.out.println("Table " + tableName + " is not available.");
			}
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			hBaseAdmin.close();
		}

		System.out.println("-----------------------------------------------");
		//readTable(conf);
		System.out.println("-----------------------------------------------");
		writeRowToTableWithJobConf(jobConf);
		System.out.println("-----------------------------------------------");
		readTable(conf);
		System.out.println("-----------------------------------------------");
		//System.exit(0);


	}

	private static void readTable(Configuration conf) {
		SparkContext sc = new SparkContext("local", "get HBase data");
		RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = sc
				.newAPIHadoopRDD(
						conf,
						TableInputFormat.class,
						org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
						org.apache.hadoop.hbase.client.Result.class);
		long count = hBaseRDD.count();
		System.out.println("Number of register in hbase table: " + count);
	}
	
	private static void writeRowToTableWithJobConf(JobConf conf) {
		JavaSparkContext sparkContext = new JavaSparkContext("local", "write data to HBase");
		//FIXME: mirar como quitar la carga de un texto arbitrario para crear un JavaRDD
		JavaRDD<String> records = sparkContext.textFile("README.md",1);
		
	    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
	    	@Override
			public Tuple2<ImmutableBytesWritable, Put> call(String t)
					throws Exception {
				Put put = new Put(Bytes.toBytes("rowkey3"));
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("c"),
						Bytes.toBytes("value3"));

				return new Tuple2<ImmutableBytesWritable, Put>(
						new ImmutableBytesWritable(), put);
			}
	      });
	    
		hbasePuts.saveAsHadoopDataset(conf);
	}
	
//	private static void writeRowToTableWithConf(Configuration conf) {
//		
//		JavaSparkContext sparkContext = new JavaSparkContext("local", "write data to HBase");
//		JavaRDD<String> records = sparkContext.textFile("README.md",1);
//		
//	    JavaPairRDD<NullWritable, Put> hbasePuts = records.mapToPair(new PairFunction<String, NullWritable, Put>() {
//	    	@Override
//			public Tuple2<NullWritable, Put> call(String t)
//					throws Exception {
//				Put put = new Put(Bytes.toBytes("rowkey3"));
//				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("c"),
//						Bytes.toBytes("value3"));
//
//				return new Tuple2<NullWritable, Put>(
//						NullWritable.get(), put);
//			}
//	      });
//	    
//		hbasePuts.saveAsNewAPIHadoopDataset(conf);
//	}

}
