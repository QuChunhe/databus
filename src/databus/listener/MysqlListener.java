package databus.listener;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

import databus.core.Listener;
import databus.core.Publisher;
import databus.event.MysqlEvent;
import databus.util.Configuration;

public class MysqlListener implements Listener{

    public MysqlListener(Publisher publisher) {
        this(publisher, CONFIG_FILE_NAME);
    }
    
    public MysqlListener(Publisher publisher, String configFileName) {
        Properties config = Configuration.instance()
                                         .loadProperties(configFileName);
        this.publisher = publisher;
        initiate(config);
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
        try {
            openRelicator.start();
        } catch (Exception e) {
            log.error("OpenRelicator throws a Exception",e);
        }
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

    private void initiate(Properties config){        
        String user = config.getProperty(USER, "root");
        String password = config.getProperty(PASSWORD, "");
        String host = config.getProperty(HOST, "127.0.0.1");
        int port = Integer.valueOf(config.getProperty(PORT, "3306"));

        int serverId = Integer.valueOf(config.getProperty(SERVER_ID, "1"));
        String binlogFileName = 
                config.getProperty(BINLOG_FILE_NAME, "master-bin.000001");
        openRelicator = new OpenReplicator();
        openRelicator.setUser(user);
        openRelicator.setPassword(password);
        openRelicator.setHost(host);
        openRelicator.setPort(port);
        openRelicator.setServerId(serverId);
        openRelicator.setBinlogFileName(binlogFileName); 
        openRelicator.setBinlogEventListener(new DatabusBinlogEventListener(this));
       
        loadPermittedTables(config);
        
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUser(user);
        ds.setPassword(password);
        ds.setServerName(host);
        ds.setPort(port);
        
        loadSchema(ds);
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
                ResultSet resultSet = metaData.getColumns(null, "%", 
                                                          tableName, "%");
                LinkedList<String> columns = new LinkedList<String>();
                LinkedList<Integer> types = new LinkedList<Integer>();
                while (resultSet.next()) {
                    int index = resultSet.getRow();
                    columns.addLast(resultSet.getString("COLUMN_NAME"));
                    types.addLast(resultSet.getInt("DATA_TYPE"));
                }
                columnsMap.put(fullName, columns);
                typesMap.put(fullName, types);
            } catch (SQLException e) {
                log.error("Cannot load the schema of "+fullName, e);
            }
        }
    }    
    
    final private static String CONFIG_FILE_NAME = "conf/listener.mysql.properties";
    
    final private static String USER = "listener.mysql.user";
    final private static String PASSWORD = "listener.mysql.password";
    final private static String HOST = "listener.mysql.host";
    final private static String PORT = "listener.mysql.port";
    final private static String SERVER_ID = "listener.mysql.serverId";
    final private static String BINLOG_FILE_NAME = "listener.mysql.binlogFileName";
    final private static String PERMITTED_TABLES = "listener.mysql.permittedTables";
    
    private static Log log = LogFactory.getLog(MysqlListener.class);
    
    private Publisher publisher;
    private OpenReplicator openRelicator;
    private Map<String, List<String>> columnsMap;
    private Map<String, List<Integer>> typesMap;
    private Set<String> permittedTableSet;
}
