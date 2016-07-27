package databus.receiver.mysql;

import java.sql.Connection;

import databus.receiver.Bean;

public interface ExecutableBean extends Bean {
    
    public void execute(Connection connection, BeanContext beanContext);

}
