package databus.listener.mysql2;

/**
 * Created by Qu Chunhe on 2018-03-19.
 */
public class TableInfo {
    public TableInfo(long serverId, String database, String table, long time) {
        this.serverId = serverId;
        this.database = database.toLowerCase();
        this.table = table.toLowerCase();
        this.time = time;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public long getTime() {
        return time;
    }

    public String getFullName() {
        return database+"."+table;
    }

    public long getServerId() {
        return serverId;
    }

    private final long serverId;
    private final String database;
    private final String table;
    private final long time;
}
