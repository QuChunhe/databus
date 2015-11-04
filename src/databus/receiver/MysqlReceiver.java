package databus.receiver;

import java.sql.Connection;
import java.sql.SQLException;
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
    
    private static Log log = LogFactory.getLog(MysqlReceiver.class);
    private DataSource dataSource = null;
}
