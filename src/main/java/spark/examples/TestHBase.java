package spark.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


/*
 * Compile and run with:
 * javac -cp `hbase classpath` TestHBase.java 
 * java -cp `hbase classpath` TestHBase
 * java -cp ./target/JavaWordCount-1.0-SNAPSHOT.jar `hbase classpath` spark.examples.TestHBase
 */

public class TestHBase {
	 public static void main(String[] args) throws Exception {
         Configuration conf = HBaseConfiguration.create();
         HBaseAdmin admin = new HBaseAdmin(conf);
         try {
             HTable table = new HTable(conf, "test-table");
             Put put = new Put(Bytes.toBytes("test-key"));
             put.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("value"));
             table.put(put);
         } finally {
             admin.close();
         }
     }

}
