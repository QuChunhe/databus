package databus.receiver.mysql;

import java.sql.Connection;

import databus.receiver.Bean;

public interface ExecutableBean extends Bean {

    /**
     * Execute MySQL commands.
     * @param connection
     * @param beanContext
     * @return sql
     */
    String execute(Connection connection, BeanContext beanContext);

}
