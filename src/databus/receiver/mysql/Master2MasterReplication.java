package databus.receiver.mysql;

import java.sql.Connection;

import databus.core.Event;
import databus.listener.mysql.DuplicateMysqlRowFilter;

public class Master2MasterReplication extends MysqlReplication {

    public Master2MasterReplication() {
        super();
    }

    @Override
    protected void receive(Connection conn, Event event) { 
        DuplicateMysqlRowFilter.put(event);
        super.receive(conn, event);
    }

}
