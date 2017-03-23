package databus.application;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DatabusStartup {

    public static void main(String[] args) throws InterruptedException {                

        String configFileName = "conf/databus.xml";
        if (args.length > 0) {
            configFileName = args[0];
        }

        String pidFileName = "data/pid"

        FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(configFileName);
        Startup startup = context.getBean("startup", Startup.class);
        startup.run(pidFile);
        context.close();

        //gracefully close log
        LogFactory.releaseAll();
    }
    
    private static Log log = LogFactory.getLog(DatabusStartup.class);
}
