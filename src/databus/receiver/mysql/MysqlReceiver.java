package databus.receiver.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    protected Connection getConnection() {   
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            log.error("Can't create Connection", e);
        }
        return connection;
    }
    
    protected int[] executeWrite(Collection<String> batchSql) {    
        int[] count = null;
        try(
            Connection conn = getConnection();
            Statement stmt = conn.createStatement();
        )
        {
            stmt.clearBatch();
            stmt.setEscapeProcessing(true);            
            for(String sql : batchSql) {
                stmt.addBatch(sql);
            }
            count = stmt.executeBatch();
        } catch (SQLException e) {
            log.error("Can't execute batch SQL : "+batchSql.toString(), e);
        } catch (Exception e) {
            log.error("throws error when execute "+batchSql.toString(), e);
        }
        
        return count;
    }
    
    protected int executeWrite(String sql) {
        int count = -1;
        try
        (
            Connection conn = getConnection();
            Statement stmt = conn.createStatement();
        )
        {           
            stmt.setEscapeProcessing(true);
            count = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("Can't execute "+sql, e);
        } catch (Exception e) {
            log.error("throws error when execute "+sql, e);
        }
        return count;
    }
    
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
