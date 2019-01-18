package databus.application;

import javax.sql.DataSource;
import java.sql.*;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.Helper;

public abstract class Mysql2Mysql {

    public Mysql2Mysql() {
        SQL = "SELECT * \n" +
              "FROM {TABLE} \n" +
              "WHERE {WHERE_CONDITION}";

    }


    public abstract void execute(String whereCondition);

    public void setTable(String table) {
        this.table = table;
    }

    public void setToDataSource(DataSource toDataSource) {
        this.toDataSource = toDataSource;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    protected void execute(DataSource fromDataSource, String whereCondition) {
        log.info("Begin migrate data between MySQL");

        String selectSQL = SQL.replace("{WHERE_CONDITION}", whereCondition)
                              .replace("{TABLE}", table);
        try (Connection conn1 = fromDataSource.getConnection();
             Statement stmt1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                     ResultSet.CONCUR_READ_ONLY);
             Connection conn2 = toDataSource.getConnection();
             Statement stmt2 = conn2.createStatement()) {
            stmt1.setFetchSize(fetchSize);

            try (ResultSet rs = stmt1.executeQuery(selectSQL)) {
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
                String sql1 = toSQL(columnNames);
                while (rs.next()) {
                    StringBuilder replaceSQL = new StringBuilder(512);
                    replaceSQL.append(sql1);
                    for (int i = 1; i <= columnCount; i++) {
                        if (1 == i) {
                            replaceSQL.append("(");
                        } else {
                            replaceSQL.append(", ");
                        }
                        String value = rs.getString(i);
                        if (null == value) {
                            replaceSQL.append("null");
                        } else if (Helper.doesUseQuotation(types[i])) {
                            replaceSQL.append("'")
                                    .append(QUOTE_PATTERN.matcher(value).replaceAll("''"))
                                    .append("'");
                        } else {
                            replaceSQL.append(value);
                        }
                    }
                    replaceSQL.append(")");
                    stmt2.execute(replaceSQL.toString());
                }
            }
        } catch (SQLException e) {
            log.error("Select MySQL Error", e);
        }
        log.info("Finish migrate data between MySQL");
    }

    private String toSQL(String[] columnNames) {
        StringBuilder sql = new StringBuilder(128);
        sql.append("REPLACE INTO ").append(table).append(" (");
        for (int i = 1; i < columnNames.length; i++) {
            if (i > 1) {
                sql.append(", ");
            }
            sql.append(columnNames[i].toLowerCase());
        }
        sql.append(")\n").append("VALUES ");

        return sql.toString();
    }

    private final static Log log = LogFactory.getLog(Mysql2Mysql.class);

    private final String SQL;
    private final Pattern QUOTE_PATTERN = Pattern.compile("'");

    private String table;
    private int fetchSize = 1000;
    private DataSource toDataSource;
}
