package databus.boot;

import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Created by Qu Chunhe on 2018-06-15.
 */
public class Mysql2CassandraMain {
    public static void main(String[] args) {
        String configFileName = "conf/databus.xml";
        if (args.length > 0) {
            configFileName = args[0];
        }
        String pidFileName = "data/pid";
        if (args.length > 1) {
            pidFileName = args[1];
        }

        FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(configFileName);
        Startup startup = context.getBean("startup", Startup.class);
        startup.run(pidFileName);
        context.close();
        //gracefully close log
        LogFactory.releaseAll();
    }
}
