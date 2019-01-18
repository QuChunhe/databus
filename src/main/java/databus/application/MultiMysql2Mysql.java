package databus.application;

import javax.sql.DataSource;
import java.util.Collection;

public class MultiMysql2Mysql extends Mysql2Mysql {
    public MultiMysql2Mysql() {
        super();
    }

    @Override
    public void execute(String whereCondition) {
        for(DataSource ds : fromDataSources) {
            execute(ds, whereCondition);
        }
    }

    public void setFromDataSources(Collection<DataSource> fromDataSources) {
        this.fromDataSources = fromDataSources;
    }

    private Collection<DataSource> fromDataSources;
}
