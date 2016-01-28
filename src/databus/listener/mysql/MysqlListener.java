package databus.listener.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.impl.AbstractBinlogParser;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import databus.event.MysqlEvent;
import databus.listener.AbstractListener;
import databus.network.Publisher;

public class MysqlListener extends AbstractListener{  
    
    public MysqlListener(Publisher publisher, Properties properties) {
        super(publisher);
        initialize(properties);        
    }

    public MysqlListener() {
        super();
    }
    
    @Override
    public boolean isRunning() {
        return (super.isRunning() && openRelicator.isRunning());
    }

    @Override
    public void stop() {
        try {
            openRelicator.stop(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("cannot stop sucessfully", e);
            openRelicator.stopQuietly(1, TimeUnit.SECONDS);
        }
        super.stop();
    }

    @Override
    public void start() {        
        if(openRelicator.isRunning()) {
            return;
        }
        try {
            openRelicator.start();
            super.start();
        } catch (Exception e) {
            log.error("OpenRelicator throws a Exception",e);
        }
    }
    
    @Override
    public void initialize(Properties properties){        
        String user = properties.getProperty("mysql.user", "root");
        String password = properties.getProperty("mysql.password", "");
        String host = properties.getProperty("mysql.host", "127.0.0.1");
        int port = Integer.valueOf(properties.getProperty("mysql.port", "3306"));
        int serverId = Integer.valueOf(properties.getProperty("mysql.serverId", "1"));
        binlogFileName = properties.getProperty("mysql.binlogFileName", "master-bin.000001");
        nextPosition = Integer.valueOf(properties.getProperty("mysql.position", "1"));
        openRelicator = new ExtendedOpenReplicator();
        openRelicator.setUser(user);
        openRelicator.setPassword(password);
        openRelicator.setHost(host);
        openRelicator.setPort(port);
        openRelicator.setServerId(serverId);
        openRelicator.setBinlogFileName(binlogFileName); 
        openRelicator.setBinlogPosition(nextPosition);
        openRelicator.setBinlogEventListener(new DatabusBinlogEventListener(this));
       
        loadPermittedTables(properties);

        MysqlDataSource ds = new MysqlDataSource();
        ds.setUser(user);
        ds.setPassword(password);
        ds.setServerName(host);
        ds.setPort(port);
        loadSchema(ds);       
    }    
    
    @Override
    protected void runOnce(boolean hasException) throws Exception {
        try {
            Thread.sleep(ONE_SECOND);
        } catch(InterruptedException e) {
            log.warn("Have been interrupted", e);
        }
        AbstractBinlogParser parser = (AbstractBinlogParser)openRelicator.getBinlogParser();
        if ((null == parser) || (!parser.isRunning())) {
            openRelicator.setRunning(false);
            openRelicator.setTransport(null);
            openRelicator.setBinlogParser(null);
            openRelicator.setBinlogPosition(nextPosition);
            openRelicator.setBinlogFileName(binlogFileName);
            openRelicator.start();
        }
    }

    public void onEvent(MysqlEvent event) {
        publisher.publish(event);
    }
    
    public boolean doPermit(String fullTableName) {
        return permittedTableSet.contains(fullTableName);
    }

    public void setNextPosition(long nextPosition) {
        this.nextPosition = nextPosition;
    }
    
    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }
    
    protected String[] getColumns(String fullName) {
        return columnsMap.get(fullName);
    }
    
    protected Integer[] getTypes(String fullName) {
        return typesMap.get(fullName);
    }
    
    protected Set<String> getPrimaryKeys(String fullName) {
        return primaryKeysMap.get(fullName);
    }
    
    private void loadPermittedTables(Properties config){        
        String rawTables = config.getProperty("mysql.permittedTables");
        if (null == rawTables) {
            log.error("mysql.permittedTables is null!");
            System.exit(1);
        }
        String[] tables = rawTables.split(",") ;
        permittedTableSet = new HashSet<String>();
        for(String t : tables) {
            permittedTableSet.add(t.trim().toLowerCase());
        }
    }
    
    private void loadSchema(MysqlDataSource ds) {
        columnsMap = new HashMap<String, String[]>();
        typesMap = new HashMap<String, Integer[]>();
        primaryKeysMap = new HashMap<String, Set<String>>();
        for(String fullName : permittedTableSet) {
            String[] r = fullName.split("\\.");
            if (r.length != 2) {
                log.error(fullName+" cannot be splitted normally");
                continue;
            }
            String databaseName = r[0].trim();
            String tableName = r[1].trim();
            ds.setDatabaseName(databaseName);
            try (Connection conn = ds.getConnection();){                
                DatabaseMetaData metaData = conn.getMetaData();  
                LinkedList<String> columns = new LinkedList<String>();
                LinkedList<Integer> types = new LinkedList<Integer>();
                try (ResultSet resultSet1 = metaData.getColumns(null, "%", tableName, "%");) {                                    
                    while (resultSet1.next()) {
                        columns.addLast(resultSet1.getString("COLUMN_NAME"));
                        types.addLast(resultSet1.getInt("DATA_TYPE"));
                    }
                }                
                columnsMap.put(fullName, columns.toArray(new String[1]));
                typesMap.put(fullName, types.toArray(new Integer[1]));
                
                HashSet<String> keys = new HashSet<String>();
                ResultSet resultSet2 = metaData.getPrimaryKeys(null, null, tableName);
                while(resultSet2.next()) {
                    keys.add(resultSet2.getString("COLUMN_NAME"));
                }
                primaryKeysMap.put(fullName, keys);                
            } catch (SQLException e) {
                log.error("Cannot load the schema of "+fullName, e);
            }
        }
    }    

    final private static long ONE_SECOND = 1000;
    
    private static Log log = LogFactory.getLog(MysqlListener.class);

    private ExtendedOpenReplicator openRelicator;
    private volatile long nextPosition;
    private volatile String binlogFileName;
    private Map<String, String[]> columnsMap;
    private Map<String, Integer[]> typesMap;
    private Map<String, Set<String>> primaryKeysMap;
    private Set<String> permittedTableSet;
    
    private static class ExtendedOpenReplicator extends OpenReplicator {
        public ExtendedOpenReplicator() {
            super();
        }
        
        public void setRunning(boolean flag) {
            running.lazySet(flag);
        }
    }
}
