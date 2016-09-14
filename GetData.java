import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetData{

    public static void main(String[] args) throws IOException, Exception{

	Configuration config= HBaseConfiguration.create();
	
	Connection connection = ConnectionFactory.createConnection(config);
	Table table= connection.getTable(TableName.valueOf("gstlal_excesspower"));


	String[] columns = {"search", "event_index", "ifo", "channel", "peak_time", "peak_time_ns", "central_freq", "start_time", "start_time_ns", "duration", "amplitude", "snr", "confidence", "chisq", "chisq_dof", "bandwidth"};
	    
	String[] columnFamilies = {"filter", "row_key", "channel", "channel", "time", "time", "frequency", "time", "time", "time", "statistics", "statistics", "statistics", "statistics", "statistics", "frequency"};
	
	// Instantiating Get class
	Get g = new Get(Bytes.toBytes("sngl_burst:event_id:4701"));
	g.addFamily(Bytes.toBytes("statistics"));
	g.addColumn(Bytes.toBytes("time"),Bytes.toBytes("peak_time"));
		
	// Reading the data
	Result result = table.get(g);

	// Reading values from Result class object
	byte [] value = result.getValue(Bytes.toBytes("statistics"),Bytes.toBytes("amplitude"));
	byte[] value1 = result.getValue(Bytes.toBytes("statistics"),Bytes.toBytes("snr"));
	byte[] value2 = result.getValue(Bytes.toBytes("statistics"),Bytes.toBytes("confidence"));
	byte[] value3 = result.getValue(Bytes.toBytes("statistics"),Bytes.toBytes("chisq"));
	byte[] value4 = result.getValue(Bytes.toBytes("statistics"),Bytes.toBytes("chisq_dof"));
	byte [] value5 = result.getValue(Bytes.toBytes("time"),Bytes.toBytes("peak_time"));

	// Printing the values
	String amplitude = Bytes.toString(value);
	String snr = Bytes.toString(value1);
	String confidence = Bytes.toString(value2);
	String chisq = Bytes.toString(value3);
	String chisq_dof = Bytes.toString(value4);
	String peak_time = Bytes.toString(value5);
	
      
	System.out.println("amp: " + amplitude + " snr: " + snr + " conf " + confidence +  " chisq " + chisq + " dof " + chisq_dof+  " t " + peak_time );
    }
}
