package databus.event;


import databus.core.Event;
import databus.core.MySQLEvent;

public abstract class MySQLAbstractEvent<T> implements MySQLEvent<T>{
    protected String ipAddress;
    protected int serverId;
    protected String databaseName;
    protected String tableName;
    protected Type type;

    @Override
    public String getIpAddress() {
        return ipAddress;
    }

    @Override
    public int getServerId() {
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
    public Type getType() {
        return type;
    }

    @Override
    public Source source() {
        return Event.Source.MYSQL;
    }

    @Override
    public String topic() {
        return source()+":"+getTableName();
    }
}
