package databus.application;

import javax.sql.DataSource;
import java.util.Collection;

/**
 * Created by Qu Chunhe on 2018-06-10.
 */
public class MultiMysql2Cassandra extends Mysql2Cassandra {
    public MultiMysql2Cassandra() {
    }

    @Override
    public void execute(String whereCondition) {
        for(DataSource ds : mysqlDataSources) {
            execute(ds, whereCondition);
        }
    }

    public void setMysqlDataSources(Collection<DataSource> mysqlDataSources) {
        this.mysqlDataSources = mysqlDataSources;
    }

    private Collection<DataSource> mysqlDataSources;
}
