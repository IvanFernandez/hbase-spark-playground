// http://autofei.wordpress.com/2012/04/02/java-example-code-using-hbase-data-model-operations/
// http://stackoverflow.com/questions/17939084/get-all-values-of-all-rows-in-hbase-using-java

package spark.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * Compile and run with:
 * java -cp ./target/hbase-spark-playground-1.0-SNAPSHOT.jar spark.examples.HBaseUtils
 */

public class HBaseUtils {
	private final static String tableName = "test";
	private final static String columnFamily = "cf";

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("file:///etc/hbase/conf.dist/hbase-site.xml"));

		HBaseAdmin hBaseAdmin = null;
		try {
			hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.isTableAvailable(tableName)) {
				System.out.println("Table " + tableName + " is available.");
			} else {
				System.out.println("Table " + tableName + " is not available.");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			hBaseAdmin.close();
		}

		// read to HBase
		getAllRecord(tableName);

		// write to HBase
		addRecord(tableName, "rowkey2", columnFamily, "b", "value2");

		// read to HBase
		getAllRecord(tableName);

		// read to HBase as list
		getAllRecordAsList(tableName);
		
		// read to HBase new API
		getAllRecordNewApi(tableName);

	}

	// read rows from table (old version)
	public static void getAllRecord(String tableName) {
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.addResource(new Path(
					"file:///etc/hbase/conf.dist/hbase-site.xml"));
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// read rows from table as list (old version)
	public static void getAllRecordAsList(String tableName) {
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.addResource(new Path(
					"file:///etc/hbase/conf.dist/hbase-site.xml"));
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner scanner = table.getScanner(s);
			for (Result result = scanner.next(); (result != null); result = scanner
					.next()) {
				for (KeyValue keyValue : result.list()) {
					System.out.println("Qualifier : " + keyValue.getKeyString()
							+ " : Value : "
							+ Bytes.toString(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// read rows from table (new version)
	public static void getAllRecordNewApi(String tableName) {
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.addResource(new Path(
					"file:///etc/hbase/conf.dist/hbase-site.xml"));
			HTable table = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); (result != null); result = scanner
					.next()) {
				Get get = new Get(result.getRow());
				Result entireRow = table.get(get);
				System.out.println(entireRow);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// put one row
	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.addResource(new Path(
					"file:///etc/hbase/conf.dist/hbase-site.xml"));
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
