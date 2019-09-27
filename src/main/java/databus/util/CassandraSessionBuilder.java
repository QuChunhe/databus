package databus.util;

import java.io.File;
/**
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

/**
 * Created by Qu Chunhe on 2018-06-10.
 */
/**
public class CassandraSessionBuilder {

    public static CqlSession build(String configFile) {
        File file = new File(System.getProperty("user.dir"), configFile);
        return CqlSession.builder()
                         .withConfigLoader(DriverConfigLoader.fromFile(file))
                         .build();
    }
}
**/
