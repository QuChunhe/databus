package databus.listener;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.OpenReplicator;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import databus.event.MysqlEvent;
import databus.network.Publisher;

public class MysqlListener extends AbstractListener{  
    
    public MysqlListener(Publisher publisher, Properties properties) {
        this.publisher = publisher;
        initialize(properties);
    }

    public MysqlListener() {
        super();
    }
    
    @Override
    public boolean isRunning() {
        return openRelicator.isRunning();
    }

    @Override
    public void stop() {
        try {
            openRelicator.stop(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("cannot stop sucessfully", e);
            openRelicator.stopQuietly(1, TimeUnit.SECONDS);
        }        
    }

    @Override
    public void start() {
        if(openRelicator.isRunning()) {
            return;
        }
        try {
            openRelicator.start();
        } catch (Exception e) {
            log.error("OpenRelicator throws a Exception",e);
        }
    }
    
    @Override
    public void initialize(Properties properties){        
        String user = properties.getProperty(USER, "root");
        String password = properties.getProperty(PASSWORD, "");
        String host = properties.getProperty(HOST, "127.0.0.1");
        int port = Integer.valueOf(properties.getProperty(PORT, "3306"));

        int serverId = Integer.valueOf(properties.getProperty(SERVER_ID, "1"));
        String binlogFileName = 
                properties.getProperty(BINLOG_FILE_NAME, "master-bin.000001");
        openRelicator = new OpenReplicator();
        openRelicator.setUser(user);
        openRelicator.setPassword(password);
        openRelicator.setHost(host);
        openRelicator.setPort(port);
        openRelicator.setServerId(serverId);
        openRelicator.setBinlogFileName(binlogFileName); 
        openRelicator.setBinlogEventListener(new DatabusBinlogEventListener(this));
       
        loadPermittedTables(properties);
        
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUser(user);
        ds.setPassword(password);
        ds.setServerName(host);
        ds.setPort(port);
        
        loadSchema(ds);
    } 
    
    public void onEvent(MysqlEvent event) {
        publisher.publish(event);        
    }
    
    public boolean doPermit(String fullTableName) {
        return permittedTableSet.contains(fullTableName);
    }
    
    protected List<String> getColumns(String fullName) {
        return columnsMap.get(fullName);
    }
    
    protected List<Integer> getTypes(String fullName) {
        return typesMap.get(fullName);
    }
    
    protected List<String> getPrimaryKeys(String fullName) {
        return primaryKeysMap.get(fullName);
    }
    
    private void loadPermittedTables(Properties config){        
        String rawTables = config.getProperty(PERMITTED_TABLES);
        if (null == rawTables) {
            log.error(PERMITTED_TABLES+" is null!");
            System.exit(1);
        }
        String[] tables = rawTables.split(",") ;
        permittedTableSet = new HashSet<String>();
        for(String t : tables) {
            permittedTableSet.add(t.trim().toLowerCase());
        }
    }
    
    private void loadSchema(MysqlDataSource ds) {
        columnsMap = new HashMap<String, List<String>>();
        typesMap = new HashMap<String, List<Integer>>();
        primaryKeysMap = new HashMap<String, List<String>>();
        for(String fullName : permittedTableSet) {
            String[] r = fullName.split("\\.");
            if (r.length != 2) {
                log.error(fullName+" cannot be splitted normally");
                continue;
            }
            String databaseName = r[0].trim();
            String tableName = r[1].trim();
            ds.setDatabaseName(databaseName);
            try {
                Connection conn = ds.getConnection();
                DatabaseMetaData metaData = conn.getMetaData();  
                
                ResultSet resultSet1 = metaData.getColumns(null, "%", 
                                                          tableName, "%");
                LinkedList<String> columns = new LinkedList<String>();
                LinkedList<Integer> types = new LinkedList<Integer>();
                while (resultSet1.next()) {
                    columns.addLast(resultSet1.getString("COLUMN_NAME"));
                    types.addLast(resultSet1.getInt("DATA_TYPE"));
                }
                columnsMap.put(fullName, columns);
                typesMap.put(fullName, types);
                
                LinkedList<String> keys = new LinkedList<String>();
                ResultSet resultSet2 = metaData.getPrimaryKeys(null, null, tableName);
                while(resultSet2.next()) {
                    keys.addLast(resultSet2.getString("COLUMN_NAME"));
                }
                primaryKeysMap.put(fullName, keys);                
            } catch (SQLException e) {
                log.error("Cannot load the schema of "+fullName, e);
            } finally {
                
            }
        }
    }    
    
    final private static String USER = "listener.mysql.user";
    final private static String PASSWORD = "listener.mysql.password";
    final private static String HOST = "listener.mysql.host";
    final private static String PORT = "listener.mysql.port";
    final private static String SERVER_ID = "listener.mysql.serverId";
    final private static String BINLOG_FILE_NAME = "listener.mysql.binlogFileName";
    final private static String PERMITTED_TABLES = "listener.mysql.permittedTables";
    
    private static Log log = LogFactory.getLog(MysqlListener.class);
    

    private OpenReplicator openRelicator;
    private Map<String, List<String>> columnsMap;
    private Map<String, List<Integer>> typesMap;
    private Map<String, List<String>> primaryKeysMap;
    private Set<String> permittedTableSet;
 
}
