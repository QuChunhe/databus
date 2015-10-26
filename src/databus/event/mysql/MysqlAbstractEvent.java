package databus.event.mysql;


import databus.event.AbstractEvent;
import databus.event.MysqlEvent;

public abstract class MysqlAbstractEvent extends AbstractEvent
                                         implements MysqlEvent{

    protected long serverId;
    protected String databaseName;
    protected String tableName;

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
        return Source.MYSQL;
    }

    @Override
    public String topic() {
        return source()+":"+serverId()+":"+
               databaseName()+":"+tableName();
    }    
    
}
