package databus.receiver;

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
        Connection conn = getConnection();
        if (null == conn) {
            log.error("Connection is null: "+batchSql.toString());
            return null;
        }
        Statement stmt = null;
        int[] count = null;
        try {
            stmt = conn.createStatement();
            stmt.clearBatch();
            stmt.setEscapeProcessing(true);            
            for(String sql : batchSql) {
                stmt.addBatch(sql);
            }
            count = stmt.executeBatch();
        } catch (SQLException e) {
            log.error("Can't execute batch SQL : "+batchSql.toString(), e);
        } finally {
            close(conn, stmt);
        }
        
        return count;
    }
    
    protected int executeWrite(String sql) {
        Connection conn = getConnection();
        if (null == conn) {
            log.error("Connection is null: "+sql);
            return -1;
        }
        Statement stmt = null;
        int count = -1;
        try {
            stmt = conn.createStatement();
            stmt.setEscapeProcessing(true);
            count = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("Can't execute "+sql, e);
        } finally {
            close(conn, stmt);
        }
        return count;
    }
    
    private void close(Connection conn, Statement stmt) {
        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.error("Can't close Statement", e);
            }
        }
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("Can't close Connectioin", e);
            }
        }
    }
    
    private static Log log = LogFactory.getLog(MysqlReceiver.class);
    
    private DataSource dataSource = null;
}
