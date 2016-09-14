import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;

public class CreateTable {
      
    public static void main(String[] args) throws IOException {
	Configuration config= HBaseConfiguration.create();
	
	Connection connection = ConnectionFactory.createConnection(config);
	Admin admin = connection.getAdmin();
	
	TableName tableName = TableName.valueOf("gstlal_excesspower");
	
	// Instantiating table descriptor class
	HTableDescriptor  tableDescriptor = new
	    HTableDescriptor(tableName);
	
	// Adding column families to table descriptor
	//sort on search
	tableDescriptor.addFamily(new HColumnDescriptor("channel"));
	tableDescriptor.addFamily(new HColumnDescriptor("time"));
	tableDescriptor.addFamily(new HColumnDescriptor("frequency"));
	tableDescriptor.addFamily(new HColumnDescriptor("statistics"));
	//other is amplitude and bandwidth

	// Execute the table through admin
	admin.createTable(tableDescriptor);
	System.out.println(" Table created ");
    }
}
