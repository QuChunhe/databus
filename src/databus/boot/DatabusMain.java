package databus.boot;

import java.io.File;

import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.apache.commons.logging.LogFactory;

public class DatabusMain {

    static {
        final String LOG_CONFIG_FILE =  System.getProperty("user.dir") + "/conf/log4j2.xml";
        if (new File(LOG_CONFIG_FILE).exists()) {
            System.setProperty("log4j.configurationFile", LOG_CONFIG_FILE);
        }
    }

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
