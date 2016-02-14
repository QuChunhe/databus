package databus.receiver.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;

public abstract class MysqlReceiver implements Receiver{
    
    @Override
    public void initialize(Properties properties) {
        properties = removePrefix(properties, "mysql.");
        try {
            dataSource = BasicDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            log.error("Can't creat DataSoruce for "+properties.toString(), e);
            System.exit(1);
        }                
    }   

    @Override
    public void receive(Event event) {
        try (Connection connection = dataSource.getConnection();){
            receive0(connection, event);
        } catch (SQLException e) {
            log.error("Can't create Connection", e);
        }        
    }

    abstract protected void receive0(Connection conn, Event event);
    
    protected Properties removePrefix(Properties originalProperties, String prefix) {
        Properties properties = new Properties();
        int prefixLength = prefix.length();
        for(String key : originalProperties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String value = originalProperties.getProperty(key);
                properties.setProperty(key.substring(prefixLength), value);
            }
        }
        
        return properties;
    }
    
    private static Log log = LogFactory.getLog(MysqlReceiver.class);
    
    private DataSource dataSource = null;
}
