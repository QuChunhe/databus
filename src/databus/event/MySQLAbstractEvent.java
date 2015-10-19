package databus.event;


import databus.core.Event;

public abstract class MySQLAbstractEvent implements MySQLEvent{

    protected long serverId;
    protected String databaseName;
    protected String tableName;


    @Override
    public long getServerId() {
        return serverId;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Source source() {
        return Event.Source.MYSQL;
    }

    @Override
    public String topic() {
        return source()+":"+getServerId()+":"+
               getDatabaseName()+":"+getTableName();
    }
}
