package databus.application;

import javax.sql.DataSource;

/**
 * Created by Qu Chunhe on 2018-06-09.
 */
public class SingleMysql2Cassandra extends Mysql2Cassandra {
    public SingleMysql2Cassandra() {
        super();
    }

    public void setMysqlDataSource(DataSource mysqlDataSource) {
        this.mysqlDataSource = mysqlDataSource;
    }

    @Override
    public void execute(String whereCondition) {
        execute(mysqlDataSource, whereCondition);
    }

    private DataSource mysqlDataSource;
}
