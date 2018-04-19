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

import databus.event.mysql.ColumnAttribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import databus.listener.RestartableListener;
import databus.util.Backup;
import databus.core.Publisher;
import databus.util.Helper;

public class MysqlListener extends RestartableListener{  
    
    public MysqlListener(Publisher publisher, Properties properties) {
        super(publisher);
        initialize(properties);        
    }

    public MysqlListener() {
        super();
    }
    
    @Override
    public boolean isRunning() {
        return  openRelicator.isRunning();
    }

    @Override
    public void restart() {
        openRelicator.restart();        
    }

    @Override
    public void stop() {
        try {
            openRelicator.stop(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Can not stop successfully", e);
            openRelicator.stopQuietly(4, TimeUnit.SECONDS);
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

    public void setConfigFileName(String configFileName) {
        initialize(Helper.loadProperties(configFileName));
    }
    
    public boolean doesPermit(String fullTableName) {
        return permittedTableSet.contains(fullTableName);
    }

    public void setNextPosition(long nextPosition) {
        openRelicator.setBinlogPosition(nextPosition);
        Backup.instance()
              .store(recordedId,
                     "mysql.position", Long.toString(nextPosition));
    }
    
    public void setBinlog(String binlogFileName, long nextPosition) {
        openRelicator.setBinlogFileName(binlogFileName);
        openRelicator.setBinlogPosition(nextPosition);
        Backup.instance()
              .store(recordedId, 
                     "mysql.binlogFileName", binlogFileName,
                     "mysql.position", Long.toString(nextPosition));
    }
    
    protected String[] getColumns(String fullName) {
        return columnsMap.get(fullName);
    }
    
    protected ColumnAttribute[] getTypes(String fullName) {
        return attributesMap.get(fullName);
    }
    
    protected Set<String> getPrimaryKeys(String fullName) {
        return primaryKeysMap.get(fullName);
    }
    
    private void loadPermittedTables(Properties config){        
        String rawTables = config.getProperty("permittedTables");
        if (null == rawTables) {
            log.error("permittedTables is null!");
            System.exit(1);
        }
        String[] tables = rawTables.split(",") ;
        for(String t : tables) {
            permittedTableSet.add(t.trim().toLowerCase());
        }
    }
    
    private void loadBackup(Properties properties) {
        Map<String, String> backup = Backup.instance().restore(recordedId);
        if (null == backup) {
            return;
        }
        String backupBinfileName =  backup.get("mysql.binlogFileName");
        if (null != backupBinfileName) {
            properties.setProperty("binlogFileName", backupBinfileName);
        }
        String backupPosition = backup.get("mysql.position");
        if (null != backupPosition) {
            properties.setProperty("position", backupPosition);
        }
    }

    private void initialize(Properties properties){
        String user = properties.getProperty("user", "root");
        String password = properties.getProperty("password", "");
        String host = properties.getProperty("host", "127.0.0.1");
        int port = Integer.valueOf(properties.getProperty("port", "3306"));
        int serverId = Integer.valueOf(properties.getProperty("serverId", "1"));

        recordedId = "MysqlListener-" + host + "-" + serverId;
        loadBackup(properties);

        String binlogFileName = properties.getProperty("binlogFileName", "master-bin.000001");
        int nextPosition = Integer.valueOf(properties.getProperty("position", "1"));
        openRelicator = new DatabusOpenReplicator();
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

        Backup.instance()
              .store(recordedId,
                     "mysql.binlogFileName", binlogFileName,
                     "mysql.position", Long.toString(nextPosition));
    }

    private void loadSchema(MysqlDataSource ds) {
        HashMap<String, Set<String>> tablesMap = new HashMap<>();
        for(String fullName : permittedTableSet) {
            String[] r = fullName.split("\\.");
            if (r.length != 2) {
                log.error(fullName+" can not be split normally");
                continue;
            }
            String databaseName = r[0].trim();
            String tableName = r[1].trim();
            Set<String> tables = tablesMap.get(databaseName);
            if (null == tables) {
                tables = new HashSet<>();
                tablesMap.put(databaseName, tables);
            }
            tables.add(tableName);
        }
        
        columnsMap = new HashMap<>();
        attributesMap = new HashMap<>();
        primaryKeysMap = new HashMap<>();
        
        for(String databaseName : tablesMap.keySet()) {
            ds.setDatabaseName(databaseName);
            String fullName = databaseName;
            try (Connection conn = ds.getConnection()){
                DatabaseMetaData metaData = conn.getMetaData();  
                Set<String> tables = tablesMap.get(databaseName);
                for(String tableName : tables) {
                    LinkedList<String> columns = new LinkedList<>();
                    LinkedList<ColumnAttribute> attribute = new LinkedList<>();
                    fullName = databaseName + "." + tableName;
                    try (ResultSet resultSet1 = metaData.getColumns(null, "%", tableName, "%")) {
                        while (resultSet1.next()) {
                            columns.addLast(resultSet1.getString("COLUMN_NAME"));
                            int type = resultSet1.getInt("DATA_TYPE");
                            String typeName = resultSet1.getString("TYPE_NAME");
                            attribute.addLast(new ColumnAttribute(type, typeName));
                        }
                    }                
                    columnsMap.put(fullName, columns.toArray(new String[columns.size()]));
                    attributesMap.put(fullName, 
                                      attribute.toArray(new ColumnAttribute[attribute.size()]));
                    
                    HashSet<String> keys = new HashSet<>();
                    ResultSet resultSet2 = metaData.getPrimaryKeys(null, null, tableName);
                    while(resultSet2.next()) {
                        keys.add(resultSet2.getString("COLUMN_NAME"));
                    }
                    primaryKeysMap.put(fullName, keys); 
                }
            } catch (SQLException e) {
                log.error("Cannot load the schema of "+fullName, e);
            }           
        } 
    }

    protected final Set<String> permittedTableSet = new HashSet<>();

    private final static Log log = LogFactory.getLog(MysqlListener.class);

    private DatabusOpenReplicator openRelicator;
    private Map<String, String[]> columnsMap;
    private Map<String, ColumnAttribute[]> attributesMap;
    private Map<String, Set<String>> primaryKeysMap;
    
    private String recordedId;
}
