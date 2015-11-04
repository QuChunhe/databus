package databus.receiver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.mysql.MysqlDeleteEvent;
import databus.event.mysql.MysqlInsertEvent;
import databus.event.mysql.MysqlUpdateEvent;

public class MysqlReplication extends MysqlReceiver{

    @Override
    public void receive(Event event) {
        if (event instanceof MysqlInsertEvent) {
            insert((MysqlInsertEvent) event);
        } else if (event instanceof MysqlUpdateEvent) {
            update((MysqlUpdateEvent)event);
        } else if (event instanceof MysqlDeleteEvent) {
            delete((MysqlDeleteEvent)event);
        } else {
            log.error("Can't process "+event.toString());
        }
    }

    private void insert(MysqlInsertEvent event) {

    }
    
    private void update(MysqlUpdateEvent event) {
        
    }
    
    private void delete(MysqlDeleteEvent event) {
        
    }
    
    private void execute(String sql) {
        Connection conn = getConnection();
        if (null == conn) {
            log.error("Connection is null: "+sql);
            return;
        }
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static Log log = LogFactory.getLog(MysqlReplication.class);

}
