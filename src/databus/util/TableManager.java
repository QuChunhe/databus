package databus.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TableManager {
    
    public static TableManager instance() {
        return instance;
    }
    
    public List<String> getColumn(String databaseName, String tableName) {
         
        return columns.get(databaseName+"."+tableName);
    }
 
    private TableManager() {
        String tableFileName = Configuration.TABLE_FILE_NAME;
        try {
            Properties tables = new Properties();
            tables.load(new FileReader(tableFileName));
            for(Entry<Object, Object> t : tables.entrySet()) {
                columns.put(t.getKey().toString(), parse(t.getValue().toString()));
            }            
        } catch (FileNotFoundException e) {
            log.error("Cannot find "+tableFileName, e);
            e.printStackTrace();
        } catch (IOException e) {
            log.error("cannot reed "+tableFileName, e);
            e.printStackTrace();
        }
    }
    
    private List<String> parse(String columnString) {
        LinkedList<String> column = new LinkedList<String>();
        int begin = columnString.indexOf('(');
        int end = columnString.indexOf(')');
        if ((-1 == begin) || (-1 == end)) {
            log.error("Cannot parse "+columnString);
        } else {
            String[] columnArray = columnString.substring(begin+1, end)
                                               .split(",");
            for (String c : columnArray) {
                column.addLast(c.trim());
            }
        }
        
        return column;
    }
    
    private static TableManager instance = new TableManager();  
    private static Log log = LogFactory.getLog(TableManager.class);
    
    private Map<String,List<String>> columns = new HashMap<String, List<String>>();
}
