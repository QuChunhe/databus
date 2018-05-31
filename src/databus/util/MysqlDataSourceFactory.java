package databus.util;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Created by Qu Chunhe on 2018-05-31.
 */
public class MysqlDataSourceFactory implements DataSourceFactory {

    @Override
    public DataSource createDataSource(String configFile) {
        Properties properties = Helper.loadProperties(configFile);
        DataSource dataSource = null;
        try {
            dataSource = BasicDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            log.error("Can not create DataSource!", e);
        }
        return  dataSource;
    }
    private final static Log log = LogFactory.getLog(MysqlDataSourceFactory.class);

}
