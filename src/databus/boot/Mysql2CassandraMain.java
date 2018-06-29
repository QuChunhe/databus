package databus.boot;

import databus.application.Mysql2Cassandra;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Created by Qu Chunhe on 2018-06-15.
 */
public class Mysql2CassandraMain {
    public static void main(String[] args) {
        System.out.println("--------------------------------------");
        if (args.length < 4) {
            System.out.println("Must has 3 parameters. The first is MySQL table, " +
                               "the second is Cassandra table, and the third is condition.");
            System.exit(1);
        }

        String configFileName = args[0];
        String mysqlTable = args[1];
        String cassandraTable = args[2];

        String condition = args[3];
        for(int i=4; i< args.length; i++) {
            condition += " AND " + args[i];
        }

        FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(configFileName);
        Mysql2Cassandra mysql2Cassandra = context.getBean("mysql2Cassandra",
                                                               Mysql2Cassandra.class);
        mysql2Cassandra.setMysqlTable(mysqlTable);
        mysql2Cassandra.setCassandraTable(cassandraTable);
        mysql2Cassandra.execute(condition);
        context.close();
    }
}
