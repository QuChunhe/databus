package databus.boot;

import databus.application.Mysql2Mysql;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class Mysql2MysqlMain {
    public static void main(String[] args) {
        System.out.println("--------------------------------------");
        if (args.length < 3) {
            System.out.println("Must has 2 parameters. The first is MySQL table, " +
                               "and the second is condition.");
            System.exit(1);
        }

        String configFileName = args[0];
        String table = args[1];


        String condition = args[2];
        for(int i=3; i< args.length; i++) {
            condition += " AND " + args[i];
        }

        FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(configFileName);
        Mysql2Mysql mysql2Mysql = context.getBean("mysql2Mysql", Mysql2Mysql.class);
        mysql2Mysql.setTable(table);
        mysql2Mysql.execute(condition);
        context.close();
    }
}
