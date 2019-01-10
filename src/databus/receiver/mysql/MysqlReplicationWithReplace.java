package databus.receiver.mysql;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import databus.core.Event;
import databus.event.MysqlEvent;

public class MysqlReplicationWithReplace extends MysqlReplication {

    public MysqlReplicationWithReplace() {
        super();
    }

    public void setDeniedTables(Collection<String> deniedTables) {
        this.deniedTables.addAll(deniedTables);
    }

    @Override
    protected void execute(Connection conn, Event event) {
        if (event instanceof MysqlEvent) {
            String table = ((MysqlEvent) event).table();
            if (deniedTables.contains(table)) {
                return;
            }
        }
        super.execute(conn, event);
    }

    @Override
    protected String getPrefixInsertSql() {
        return "REPLACE INTO ";
    }

    private final Set<String> deniedTables = new HashSet<>();
}
