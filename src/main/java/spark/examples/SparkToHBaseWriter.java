package spark.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class SparkToHBaseWriter {
	
	private final static String tableName = "test";
	private final static String columnFamily = "cf";

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("file:///etc/hbase/conf/hbase-site.xml");
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
		//conf.set(TableInputFormat.SCAN_COLUMNS, columnFamily);
		
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.isTableAvailable(tableName)) {
				System.out.println("Table " + tableName + " is available.");
			}
			else {
				System.out.println("Table " + tableName + " is not available.");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		SparkContext sc = new SparkContext("local", "get HBase data");
		RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = sc
				.newAPIHadoopRDD(
						conf,
						TableInputFormat.class,
						org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
						org.apache.hadoop.hbase.client.Result.class);
		long count = hBaseRDD.count();
		System.out.println("Number of register in hbase table: " + count);
		System.exit(0);

	}

}
