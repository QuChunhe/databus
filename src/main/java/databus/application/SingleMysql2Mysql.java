package databus.application;

import javax.sql.DataSource;

public class SingleMysql2Mysql extends Mysql2Mysql {
    public SingleMysql2Mysql() {
        super();
    }

    public void setFromDataSource(DataSource fromDataSource) {
        this.fromDataSource = fromDataSource;
    }

    @Override
    public void execute(String whereCondition) {
        execute(fromDataSource, whereCondition);
    }

    private DataSource fromDataSource;
}
