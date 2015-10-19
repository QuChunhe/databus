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
    
    public List<String> getColumns(String databaseName, String tableName) {
        LinkedList<String> columns = new LinkedList<String>();
        
        return columns;
    }
 
    private TableManager() {
        try {
            Properties tables = new Properties();
            tables.load(new FileReader(CONFIG_FILE_NAME));
            for(Entry<Object, Object> t : tables.entrySet()) {
                
            }
        } catch (FileNotFoundException e) {
            log.error("Cannot find "+CONFIG_FILE_NAME, e);;
        } catch (IOException e) {
            log.error("cannot reed "+CONFIG_FILE_NAME, e);
        }
    }
    
    private List<String> parse(String columnString) {
        LinkedList<String> column = new LinkedList<String>();
        
        String[] columnArray = 
                columnString.substring(1, columnString.length()).split(",");
        
        return column;
    }

    private static final String CONFIG_FILE_NAME = "tables.properties";
    
    private static TableManager instance = new TableManager();  
    private static Log log = LogFactory.getLog(TableManager.class);
    
    private Map<String,List<String>> columns = new HashMap<String, List<String>>();
}
