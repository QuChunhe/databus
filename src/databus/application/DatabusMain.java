package databus.application;

import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.apache.commons.logging.LogFactory;

public class DatabusMain {

    public static void main(String[] args) throws InterruptedException {
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
