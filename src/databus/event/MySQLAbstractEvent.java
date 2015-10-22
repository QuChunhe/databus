package databus.event;


import databus.core.Event;

public abstract class MySQLAbstractEvent implements MySQLEvent{

    protected long serverId;
    protected String databaseName;
    protected String tableName;
    protected long time;


    @Override
    public long time() {
        return time;
    }

    @Override
    public long serverId() {
        return serverId;
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public Source source() {
        return Event.Source.MYSQL;
    }

    @Override
    public String topic() {
        return source()+":"+serverId()+":"+
               databaseName()+":"+tableName();
    }
    
}
