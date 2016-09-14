import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;


public class ReadFileAndInsert {

    public static void main(String[] args) {
	String csvFile = "/home/hduser/LIGOdatabase/event.csv";
	BufferedReader br = null;
	String line = "";
	String csvSplitBy = ",";

	String[] columns = {"search", "event_index", "ifo", "channel", "peak_time", "peak_time_ns", "central_freq", "start_time", "start_time_ns", "duration", "amplitude", "snr", "confidence", "chisq", "chisq_dof", "bandwidth"};
	    
	String[] columnFamilies = {"filter", "row_key", "channel", "channel", "time", "time", "frequency", "time", "time", "time", "statistics", "statistics", "statistics", "statistics", "statistics", "frequency"};
	
	
	String filterValue = "gstlal_excesspower";
	int filterNumber = 0;
	int keyNumber = 1;


	try{

	    Configuration config = HBaseConfiguration.create();
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf("gstlal_excesspower"));
	    
	
	    
	    br = new BufferedReader(new FileReader(csvFile));
	    while((line = br.readLine()) !=null)
		{
                    System.out.println(line);
		    String[] event = line.split(csvSplitBy, -1); // -1 includes empty strings
		    if(event.length!=columns.length){
			System.out.println("columns length error");
		    }
		    if(event.length!=columnFamilies.length){
			System.out.println("columnFamilies length error");
		    }
		    if(event[filterNumber].equals(filterValue)){
			Put p = new Put(Bytes.toBytes(event[1]));
			for(int i=keyNumber+1; i<event.length; i++){
			    p.add(Bytes.toBytes(columnFamilies[i]),
				  Bytes.toBytes(columns[i]),
				  Bytes.toBytes(event[i]));
			}
			table.put(p);
                        System.out.println(p);
                        System.out.println("PUT");
		    }
		}
	    //System.out.println(event[1]+ " " + event[2]+ " " + event[3]+ " "+ event[5]+ " " + event[6] + " " + event[7]);
	
	}catch (FileNotFoundException e){
	    e.printStackTrace();
	}catch (IOException e) {
	    e.printStackTrace();
	}finally{

	    if(br!=null){
		try{
		    br.close();
		} catch(IOException e){
		    e.printStackTrace();
		}
	    }
	}
    }
}
