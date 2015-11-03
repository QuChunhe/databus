package databus.subscriber;

import java.sql.Connection;

public abstract class MysqlSubscriber extends AbstractSubscriber{
    
    protected Connection getConnection() {
        return null;
    }

}
