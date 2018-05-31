package databus.receiver.mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;

public abstract class MysqlReceiver implements Receiver {

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void receive(final Event event) {
        try (Connection connection = dataSource.getConnection()){
            execute(connection, event);
        } catch (SQLException e) {
            log.error("Can not create Connection", e);
        }
    }

    @Override
    public void close() throws IOException {
    }

    abstract protected void execute(Connection conn, final Event event);

    private final static Log log = LogFactory.getLog(MysqlReceiver.class);
    
    private DataSource dataSource = null;
}
