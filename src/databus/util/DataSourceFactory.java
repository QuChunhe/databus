package databus.util;

import javax.sql.DataSource;

/**
 * Created by Qu Chunhe on 2017-01-03.
 */
public interface DataSourceFactory {

    DataSource createDataSource(String configFile);
}
