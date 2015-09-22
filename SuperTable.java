import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instaniate HBaseAdmin class
      HBaseAdmin admin = new HBaseAdmin(config);

      // Instantiate table descriptor class
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("powers"));

      // Add column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);
      System.out.println("Table Created");

      // Instantiating HTable class
      HTable hTable = new HTable(config, "powers");

      // Repeat these steps as many times as necessary

	      // Instantiating Put class
              // Hint: Accepts a row name

      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value

      Put superman = new Put(Bytes.toBytes("row1"));

      superman.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("superman"));
      superman.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("strength"));
      superman.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("clark"));
      superman.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("100"));

      Put batman = new Put(Bytes.toBytes("row2"));
      batman.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("batman"));
      batman.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("money"));
      batman.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("bruce"));
      batman.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("50"));

      Put wolverine = new Put(Bytes.toBytes("row3"));
      wolverine.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("wolverine"));
      wolverine.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("healing"));
      wolverine.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("logan"));
      wolverine.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("75"));

      // Save the table
      hTable.put(superman);
      hTable.put(batman);
      hTable.put(wolverine);

      // Close table
      hTable.close();

      // Instantiate the Scan class
      Scan scan = new Scan();

      // Scan the required columns
      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

      // Get the scan result
      ResultScanner scanner = hTable.getScanner(scan);

      // Read values from scan result
      // Print scan result
      for (Result result = scanner.next(); result != null; result = scanner.next())
        System.out.println("Got row: " + result);

      // Close the scanner
      scanner.close();

      // Htable closer
      hTable.close();
   }
}
