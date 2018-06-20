package databus.application;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2018-06-10.
 */
public abstract class Mysql2Cassandra {

    public Mysql2Cassandra() {
        SQL = "SELECT {COLUMN_LIST}\n" +
              "INTO OUTFILE '{OUT_FILE}'\n" +
              "        FIELDS TERMINATED BY ','  \n" +
              "               OPTIONALLY ENCLOSED BY \"'\"\n" +
              "               ESCAPED BY \"'\"\n"+
              "        LINES TERMINATED BY '\\n'\n" +
              "FROM {MYSQL_TABLE} \n" +
              "WHERE {WHERE_CONDITION}";
        CQL = "INSERT INTO {CASSANDRA_TABLE} {COLUMN_LIST}\n" +
              "VALUES ({VALUE});";
    }

    public abstract void execute(String whereCondition);

    public void setMysqlTable(String mysqlTable) {
        this.mysqlTable = mysqlTable;
    }

    public void setCassandraTable(String cassandraTable) {
        this.cassandraTable = cassandraTable;
    }

    public void setCassandraCluster(Cluster cassandraCluster) {
        this.cassandraCluster = cassandraCluster;
    }

    public void setColumnMap(Map<String, String> columnMap) {
        this.columnMap.putAll(columnMap);
    }

    public void setTmpDirectory(String tmpDirectory) {
        if (!tmpDirectory.endsWith("/")) {
            tmpDirectory = tmpDirectory+"/";
        }
        this.tmpDirectory = tmpDirectory;
    }

    protected void execute(DataSource mysqlDataSource, String whereCondition) {
        log.info("Begin migrate data from "+mysqlTable+" to "+cassandraTable);
        String outFile = createOutFile();
        Set<String> mysqlColumns = getMysqlTableColumns(mysqlDataSource);
        exportMysql(mysqlDataSource, whereCondition, outFile, mysqlColumns);
        int cassandraRowCount = importCassandra(mysqlColumns, outFile);
        log.info("Cassandra import "+cassandraRowCount+" rows!");
        try {
            Files.deleteIfExists(Paths.get(outFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void exportMysql(DataSource mysqlDataSource, String whereCondition,
                               String outFile, Set<String> mysqlColumns) {
        String sql = SQL.replace("{COLUMN_LIST}", toMysqlColumnString(mysqlColumns))
                        .replace("{OUT_FILE}", outFile)
                        .replace("{WHERE_CONDITION}", whereCondition)
                        .replace("{MYSQL_TABLE}", mysqlTable);

        try(Connection conn = mysqlDataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            log.error("Can not export data from MySQL", e);
            return;
        }
    }

    protected int importCassandra(Set<String> mysqlColumns, String inFile) {
        String cassandraColumnString = toCassandraColumnString(toCassandraColumns(mysqlColumns));
        String cqlTemplate = CQL.replace("{COLUMN_LIST}", cassandraColumnString)
                                .replace("{CASSANDRA_TABLE}", cassandraTable);
        int rowCount = 0;
        try (Session session = cassandraCluster.connect();
             BufferedReader reader = Files.newBufferedReader(Paths.get(inFile), StandardCharsets.UTF_8)) {
            String line;
            while ((line=reader.readLine()) != null) {
                String cql =  cqlTemplate.replace("{VALUE}", line);
                try {
                    session.execute(cql);
                    rowCount++;
                } catch (Exception e) {
                    log.error("Can not insert row :"+cql, e);
                }
            }
        } catch (Exception e) {
            log.error("Can not import data", e);
        }
        return rowCount;
    }

    protected String createOutFile() {
        return tmpDirectory + mysqlTable + "_" + LocalDate.now().toString() + "_" +
               ThreadLocalRandom.current().nextLong()+".csv";
    }

    private Set<String> getMysqlTableColumns(DataSource mysqlDataSource) {
        HashSet<String> columns = new HashSet<>();
        try (Connection conn = mysqlDataSource.getConnection()){
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(null, "%", mysqlTable, "%")) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME").toLowerCase());
                }
            } catch (SQLException e) {
                log.error("Can not get columns for " + mysqlTable, e);
            }
        } catch (SQLException e) {
            log.error("Can not get connection "+mysqlTable, e);
        }
        return columns;
    }

    private Set<String> toCassandraColumns(Set<String> mysqlColumns) {
        HashSet<String> columns = new HashSet<>();
        for(String c : mysqlColumns) {
            String r = columnMap.get(c);
            if (null == r) {
                columns.add(c);
            } else {
                columns.add(r);
            }
        }
        return columns;
    }

    private String toCassandraColumnString(Set<String> columns) {
        StringBuilder builder = new StringBuilder(128);
        for(String c : columns) {
            if (builder.length() == 0) {
                builder.append("(");
            } else {
                builder.append(", ");
            }
            builder.append(c);
        }
        builder.append(")");
        return builder.toString();
    }

    private String toMysqlColumnString(Set<String> columns) {
        StringBuilder builder = new StringBuilder(128);
        for(String c : columns) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(c);
        }
        return builder.toString();
    }

    private final static Log log = LogFactory.getLog(Mysql2Cassandra.class);

    private final String SQL;
    private final String CQL;

    private String mysqlTable;
    private String cassandraTable;

    private Cluster cassandraCluster;

    private String tmpDirectory = "/tmp/";

    private Map<String, String> columnMap = new HashMap<>();
}
