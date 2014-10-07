package spark.examples;

import java.io.IOException;

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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

//http://codereview.stackexchange.com/questions/56641/producing-a-sorted-wordcount-with-spark

public class SparkToHBase {
	
	private final static String tableName = "test";
	private final static String columnFamily = "cf";

	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("file:///etc/hbase/conf.dist/hbase-site.xml"));
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		
		// new Hadoop API configuration
		Job newAPIJobConfiguration = Job.getInstance(conf);
		newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		
		// old Hadoop API configuration
		JobConf oldAPIJobConfiguration = new JobConf(conf, SparkToHBase.class);
		oldAPIJobConfiguration.setOutputFormat(TableOutputFormat.class);
		oldAPIJobConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		
		
		
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
		readTable(conf);
		System.out.println("-----------------------------------------------");
		writeRowOldHadoopAPI(oldAPIJobConfiguration);
		writeRowNewHadoopAPI(newAPIJobConfiguration.getConfiguration());
		System.out.println("-----------------------------------------------");
		readTable(conf);
		System.out.println("-----------------------------------------------");


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
	
	private static void writeRowOldHadoopAPI(JobConf conf) {
		JavaSparkContext sparkContext = new JavaSparkContext("local", "write data to HBase");
		//FIXME: mirar como quitar la carga de un texto arbitrario para crear un JavaRDD
		JavaRDD<String> records = sparkContext.textFile("README.md",1);
		
	    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
	    	@Override
			public Tuple2<ImmutableBytesWritable, Put> call(String t)
					throws Exception {
				Put put = new Put(Bytes.toBytes("rowkey7"));
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("z"),
						Bytes.toBytes("value3"));

				return new Tuple2<ImmutableBytesWritable, Put>(
						new ImmutableBytesWritable(), put);
			}
	      });
		hbasePuts.saveAsHadoopDataset(conf);
	}
	
	private static void writeRowNewHadoopAPI(Configuration conf) {
		
		JavaSparkContext sparkContext = new JavaSparkContext("local", "write data to HBase");
		JavaRDD<String> records = sparkContext.textFile("README.md",1);
		//FIXME: mirar como quitar la carga de un texto arbitrario para crear un JavaRDD
	    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
	    	@Override
			public Tuple2<ImmutableBytesWritable, Put> call(String t)
					throws Exception {
				Put put = new Put(Bytes.toBytes("rowkey6"));
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("z"),
						Bytes.toBytes("value3"));

				return new Tuple2<ImmutableBytesWritable, Put>(
						new ImmutableBytesWritable(), put);
			}
	      });
	    
		hbasePuts.saveAsNewAPIHadoopDataset(conf);
	}

}
