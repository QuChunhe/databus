package databus.receiver.mysql;

import java.sql.Connection;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class MysqlReceiver4Redis extends AbstractMysqlReceiver4Redis {
    public MysqlReceiver4Redis() {
        super();
    }

    public void setMysqlBean(MysqlBean mysqlBean) {
        this.mysqlBean = mysqlBean;
    }

    @Override
    protected void execute(Connection connection, String key, String message) {
        mysqlBean.execute(connection, key, message);
    }

    private MysqlBean mysqlBean;
}
