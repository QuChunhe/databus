package databus.receiver.mysql;

import java.sql.Connection;

/**
 * Created by Qu Chunhe on 2018-05-18.
 */
public interface MessageBean {

    void execute(Connection connection, String key, String message);
}
