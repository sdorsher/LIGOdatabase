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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import java.util.List;


public class FilterString{

    public static void main(String[] args) throws IOException, Exception{

	Configuration config= HBaseConfiguration.create();
	
	Connection connection = ConnectionFactory.createConnection(config);
	Table table= connection.getTable(TableName.valueOf("gstlal_excesspower"));


	String[] columns = {"search", "event_index", "ifo", "channel", "peak_time", "peak_time_ns", "central_freq", "start_time", "start_time_ns", "duration", "amplitude", "snr", "confidence", "chisq", "chisq_dof", "bandwidth"};
	    
	String[] columnFamilies = {"filter", "row_key", "channel", "channel", "time", "time", "frequency", "time", "time", "time", "statistics", "statistics", "statistics", "statistics", "statistics", "frequency"};
	
	SingleColumnValueFilter filter =
	     new SingleColumnValueFilter(Bytes.toBytes(columnFamilies[3]),
	    				Bytes.toBytes(columns[3]),
	    				CompareFilter.CompareOp.EQUAL,
	    				new SubstringComparator("STRAIN"));

	    	    
	Scan scan = new Scan();
	scan.setFilter(filter);
	ResultScanner scanner =table.getScanner(scan);
	for(Result result: scanner){
	    List<Cell> colCells = result.getColumnCells(Bytes.toBytes(columnFamilies[3]),Bytes.toBytes(columns[3]));
	    for(Cell cell : colCells){
		System.out.println("Cell:" + cell + ", Value: " +
				   Bytes.toString(cell.getValueArray(),
						  cell.getValueOffset(),
						  cell.getValueLength()));
	    }
	}
	scanner.close();

		
    }
}
	    
