package databus.event.mysql;


import databus.event.AbstractEvent;
import databus.event.MysqlEvent;

public abstract class MysqlAbstractEvent extends AbstractEvent
                                         implements MysqlEvent {

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
    
    public MysqlAbstractEvent serverId(long serverId) {
        this.serverId = serverId;
        return this;
    }
    
    public MysqlAbstractEvent databaseName(String databaseName) {
        this.databaseName = databaseName.toUpperCase();
        return this;
    }
    
    public MysqlAbstractEvent tableName(String tableName) {
        this.tableName = tableName.toUpperCase();
        return this;
    }
    
    private long serverId;
    private String databaseName;
    private String tableName;    
}
