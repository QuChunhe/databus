package databus.boot;

import databus.application.MultiMysql2Cassandra;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.util.Arrays;

/**
 * Created by Qu Chunhe on 2018-06-15.
 */
public class Mysql2CassandraMain {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Must has 3 parameters. The first is MySQL table, " +
                               "the second is Cassandra table, and the third is condition.");
            System.exit(1);
        }
        String mysqlTable = args[0];
        String cassandraTable = args[1];
        String condtion = args[2];
        System.out.println(Arrays.toString(args));
        String configFileName = "conf/data_migration.xml";

        FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(configFileName);
        MultiMysql2Cassandra mysql2Cassandra = context.getBean("multiMysql2Cassandra",
                                                               MultiMysql2Cassandra.class);
        mysql2Cassandra.setMysqlTable(mysqlTable);
        mysql2Cassandra.setCassandraTable(cassandraTable);
        mysql2Cassandra.execute(condtion);
        context.close();
    }
}
