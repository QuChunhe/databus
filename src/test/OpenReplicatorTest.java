package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class OpenReplicatorTest {
	//
	private static final Logger LOGGER = LoggerFactory.getLogger(OpenReplicatorTest.class);

	/**
	 *
	 */
	public static void main(String args[]) throws Exception {

	    Gson gson = new GsonBuilder().enableComplexMapKeySerialization() 
                .serializeNulls()   
                .setDateFormat(DateFormat.LONG)
                .create();
		
		final OpenReplicator or = new OpenReplicator(); 
		or.setUser("root");
		or.setPassword("quchunhe");
		or.setHost("127.0.0.1");
		or.setPort(3306);
		or.setServerId(1);
		or.setBinlogPosition(120);
		or.setBinlogFileName("master-bin.000001");
		or.setBinlogEventListener(new BinlogEventListener() {
		    public void onEvents(BinlogEventV4 event) {
		        System.out.println("new event:");
		        System.out.println(event.getClass().getSimpleName());
		    	System.out.println(event);

		    	System.out.println(" ");
		    	System.out.println(" ");
		    }
		});
		or.start();

		//
		LOGGER.info("press 'q' to stop");
		final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		for(String line = br.readLine(); line != null; line = br.readLine()) {
		    if(line.equals("q")) {
		        or.stop(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		        break;
		    }
		}
	}
}
