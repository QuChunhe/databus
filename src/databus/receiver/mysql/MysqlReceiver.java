package databus.receiver.mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.util.Benchmark;
import databus.util.Helper;

public abstract class MysqlReceiver implements Receiver{

    public void setConfigFile(String configFile) {
        Properties properties = Helper.loadProperties(configFile);
        try {
            dataSource = BasicDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            log.error("Can not create DataSource for "+properties.toString(), e);
            System.exit(1);
        }                
    }   

    @Override
    public void receive(Event event) {
        try (Connection connection = dataSource.getConnection()){
            Benchmark benchmark = new Benchmark();
            String sql = execute(connection, event);
            if (null != sql) {
                log.info(benchmark.elapsedMsec(4)+"ms execute : "+sql);
            }
        } catch (SQLException e) {
            log.error("Can not create Connection", e);
        }
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * Execute MySQL commands.
     * @param conn
     * @param event
     * @return sql
     */
    abstract protected String execute(Connection conn, Event event);

    private final static Log log = LogFactory.getLog(MysqlReceiver.class);
    
    private DataSource dataSource = null;
}
