package databus.event.mysql;


import databus.event.AbstractEvent;
import databus.event.MysqlEvent;

public abstract class AbstractMysqlEvent extends AbstractEvent
                                         implements MysqlEvent {

    @Override
    public long serverId() {
        return serverId;
    }

    @Override
    public String database() {
        return database;
    }

    @Override
    public String table() {
        return table;
    }

    @Override
    public Source source() {
        return Source.MYSQL;
    }

    @Override
    public String topic() {
        return source()+":"+serverId()+":"+database();
    }
    
    public AbstractMysqlEvent serverId(long serverId) {
        this.serverId = serverId;
        return this;
    }
    
    public AbstractMysqlEvent database(String database) {
        this.database = database.toLowerCase();
        return this;
    }
    
    public AbstractMysqlEvent table(String table) {
        this.table = table.toLowerCase();
        return this;
    }
    
    private long serverId;
    private String database;
    private String table;    
}
