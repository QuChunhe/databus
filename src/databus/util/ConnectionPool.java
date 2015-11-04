package databus.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.example.Configuration;

public class ConnectionPool {
    
    public static ConnectionPool instance() {
        return instance;
    }
    
    public Connection getConnection(String name){
        DataSource ds = dataSourcesMap.get(name);
        Connection conn = null;
        if (null == ds) {
            log.error("Can not find DataSource for "+name);
        } else {
            try {
                conn = ds.getConnection();
            } catch (SQLException e) {
                log.error("Cannot create Connection for "+name, e);
            }
        }
        return conn;
    }    
    
    private ConnectionPool() {
        Map<String, Properties> propertiesMap = 
                                Configuration.instance().loadMysqlProperties();
        dataSourcesMap = new HashMap<String,DataSource>();
        
        for(String key : propertiesMap.keySet()) {
            Properties properties = propertiesMap.get(key);
            if (null == properties) {
                log.error("Cannot find properties for "+key);
                continue;
            }
            DataSource ds = null;
            try {
                ds = BasicDataSourceFactory.createDataSource(properties);
            } catch (Exception e) {
                log.error("Cannot creat DataSoruce for "+key, e);
            }
            if (null != ds) {
                dataSourcesMap.put(key, ds);
            }
        }
    }
    
    private static Log log = LogFactory.getLog(ConnectionPool.class);
    private static ConnectionPool instance = new ConnectionPool();
    
    private Map<String, DataSource> dataSourcesMap;
}
