package databus.boot;

import databus.application.Mysql2Redis;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Created by Qu Chunhe on 2020-01-16.
 */
public class Mysql2RedisMain {
    public static void main(String[] args) {
        System.out.println("--------------------------------------");
        if (args.length < 2) {
            System.out.println("Must has 2 parameters. The first is configuration file, the second is MySQL table, " +
                    "and the third is condition optionally.");
            System.exit(1);
        }

        String configFileName = args[0];
        String table = args[1];
        String condition = args.length>= 3 ? args[2] : "";

        FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(configFileName);
        Mysql2Redis mysql2Redis = context.getBean("mysql2Redis", Mysql2Redis.class);

        mysql2Redis.execute(table, condition);
        context.close();
    }
}
