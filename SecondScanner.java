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
import org.apache.hadoop.hbase.filter.FilterList;
import java.util.ArrayList;
import org.apache.hadoop.hbase.filter.RowFilter;
import java.nio.ByteBuffer;

public class SecondScanner{

    public static void main(String[] args) throws IOException, Exception{

	Configuration config= HBaseConfiguration.create();
	
	Connection connection = ConnectionFactory.createConnection(config);
	Table table= connection.getTable(TableName.valueOf("gstlal_excesspower"));


	String[] columns = {"search", "event_index", "ifo", "channel", "peak_time", "peak_time_ns", "central_freq", "start_time", "start_time_ns", "duration", "amplitude", "snr", "confidence", "chisq", "chisq_dof", "bandwidth"};
	    
	String[] columnFamilies = {"filter", "row_key", "channel", "channel", "time", "time", "frequency", "time", "time", "time", "statistics", "statistics", "statistics", "statistics", "statistics", "frequency"};

	FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	
	SingleColumnValueFilter snrgt1 =

	  new SingleColumnValueFilter(Bytes.toBytes(columnFamilies[11]),
					Bytes.toBytes(columns[11]),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
						new BinaryComparator(Bytes.toBytes("0.97000000")));
	filters.addFilter(snrgt1);
	
	SingleColumnValueFilter chanSTRAIN = 
	  new SingleColumnValueFilter(Bytes.toBytes(columnFamilies[3]),
	  				Bytes.toBytes(columns[3]),
	  				CompareFilter.CompareOp.EQUAL,
	  				new SubstringComparator("STRAIN"));
	filters.addFilter(chanSTRAIN);

	RowFilter rowlt5000= new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes("sngl_burst:event_id:5000")));
	filters.addFilter(rowlt5000);

	RowFilter rowgt4800 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
					 new BinaryComparator(Bytes.toBytes("sngl_burst:event_id:4800")));
	filters.addFilter(rowgt4800);
	
	
	    
	Scan scan = new Scan();
	scan.setFilter(filters);
	ResultScanner scanner =table.getScanner(scan);
	List<Cell> finalCells = new ArrayList<Cell>();
	for(Result result: scanner){
	    Cell cell = result.getColumnLatestCell(Bytes.toBytes(columnFamilies[12]),Bytes.toBytes(columns[12]));
	    if (ByteBuffer.wrap(cell.getValueArray()).getDouble()
		>=0.000000){
		System.out.println(ByteBuffer.wrap(cell.getValueArray()).getDouble());

		finalCells.add(cell);
		finalCells.add(result.getColumnLatestCell(Bytes.toBytes(columnFamilies[11]),Bytes.toBytes(columns[11])));
	    }
	}
	scanner.close();
	for(Cell cl: finalCells){	
	    System.out.println("Cell:" + cl + ", Value: " +
			       Bytes.toString(cl.getValueArray(),
					      cl.getValueOffset(),
					      cl.getValueLength()));
	}

		
    }
}
	    
