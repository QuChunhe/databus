package databus.application;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.Helper;
import databus.util.Callback;
import databus.util.FutureChecker;
import databus.util.OperationCounter;

/**
 * Created by Qu Chunhe on 2018-06-10.
 */
public abstract class Mysql2Cassandra {

    public Mysql2Cassandra() {
        SQL = "SELECT * \n" +
              "FROM {MYSQL_TABLE} \n" +
              "WHERE {WHERE_CONDITION}";
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

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public void setFutureChecker(FutureChecker futureChecker) {
        this.futureChecker = futureChecker;
    }

    protected void execute(DataSource mysqlDataSource, String whereCondition) {
        log.info("Begin migrate data from "+mysqlTable+" to "+cassandraTable);

        String sql = SQL.replace("{WHERE_CONDITION}", whereCondition)
                        .replace("{MYSQL_TABLE}", mysqlTable);

        OperationCounter counter = new OperationCounter();
        try (Session session = cassandraCluster.connect();
             Connection conn = mysqlDataSource.getConnection();
             Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                   ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(fetchSize);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                rs.setFetchSize(fetchSize);
                ResultSetMetaData metaData = rs.getMetaData();

                int columnCount = metaData.getColumnCount();
                int[] types = new int[columnCount + 1];
                for (int i = 1; i <= columnCount; i++) {
                    types[i] = metaData.getColumnType(i);
                }
                String[] columnNames = new String[columnCount + 1];
                for (int i = 1; i <= columnCount; i++) {
                    columnNames[i] = metaData.getColumnName(i);
                }
                String CQL = toCQL(columnNames);
                while (rs.next()) {
                    StringBuilder cql = new StringBuilder(CQL.length());
                    cql.append(CQL);
                    for (int i = 1; i <= columnCount; i++) {
                        if (1 == i) {
                            cql.append("(");
                        } else {
                            cql.append(", ");
                        }
                        String value = rs.getString(i);
                        if (null == value) {
                            cql.append("null");
                        } else if (Helper.doesUseQuotation(types[i])) {
                            cql.append("'")
                                    .append(QUOTE_PATTERN.matcher(value).replaceAll("''"))
                                    .append("'");
                        } else {
                            cql.append(value);
                        }
                    }
                    cql.append(")");
                    counter.addTotalCount(1);
                    if (null == futureChecker) {
                        try {
                            session.execute(cql.toString());
                            counter.addSuccessCount(1);
                        } catch (Exception e) {
                            log.error("Can not inset a row : " + cql.toString(), e);
                            counter.addFailureCount(1);
                        }
                    } else {
                        ResultSetFuture future = session.executeAsync(cql.toString());
                        futureChecker.check(future, new Callback<com.datastax.driver.core.ResultSet>() {
                            @Override
                            public void onFailure(Throwable t) {
                                log.error("Can not inset a row : " + cql.toString(), t);
                                counter.addFailureCount(1);
                            }

                            @Override
                            public void onSuccess(com.datastax.driver.core.ResultSet rows) {
                                counter.addSuccessCount(1);
                            }
                        });
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Select MySQL Error", e);
        }

        try {
            counter.waitOnCompletion(60000);

        }catch (InterruptedException e) {
            log.error("Can not wait on completion!", e);
        }
        if (counter.getTotalCount() == (counter.getSuccessCount()+counter.getFailureCount())) {
            log.info("MySQL export "+counter.getTotalCount()+" rows, " +
                     "Cassandra import "+counter.getSuccessCount()+" rows.");
        } else {
            log.info("MySQL export "+counter.getTotalCount()+" rows, " +
                     "Cassandra import "+counter.getSuccessCount()+" rows." +
                     "but failed insertion "+counter.getFailureCount()+" rows");
        }

    }

    private String toCQL(String[] columnNames)  {
        StringBuilder cql = new StringBuilder(256);
        cql.append("INSERT INTO ").append(cassandraTable).append(" (");
        for (int i=1; i< columnNames.length; i++) {
            if (i > 1) {
                cql.append(", ");
            }
            cql.append(toCassandraColumn(columnNames[i].toLowerCase()));
        }
        cql.append(")\n")
           .append("VALUES ");

        return cql.toString();
    }


    private String toCassandraColumn(String mysqlColumn) {
        return columnMap.getOrDefault(mysqlColumn, mysqlColumn);
    }


    private final static Log log = LogFactory.getLog(Mysql2Cassandra.class);

    private final String SQL;
    private final Pattern QUOTE_PATTERN = Pattern.compile("'");

    private String mysqlTable;
    private String cassandraTable;
    private Cluster cassandraCluster;
    private int fetchSize = 10000;
    private Map<String, String> columnMap = new HashMap<>();
    private FutureChecker futureChecker;
}
