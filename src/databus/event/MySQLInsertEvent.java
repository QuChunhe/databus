package databus.event;

import java.util.List;

public class MySQLInsertEvent extends MySQLWriteEvent<List<String>>{    

    public MySQLInsertEvent(long serverId, 
                            String databaseName,
                            String tableName) {
        super(serverId, databaseName, tableName);
    }

    @Override
    public Type type() {
        return MySQLEvent.Type.INSERT;
    }

}
