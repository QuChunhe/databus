package databus.application;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashSet;
import java.util.LinkedList;

import databus.receiver.redis2.RedisReceiver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.mysql.Column;
import databus.event.mysql.MysqlInsertRow;

/**
 * Created by Qu Chunhe on 2020-01-16.
 */
public class Mysql2Redis {

    public Mysql2Redis() {
        SQL = "SELECT * \n" +
              "FROM {MYSQL_TABLE}\n" +
              "{WHERE_CONDITION}";
    }

    public void execute(String table, String whereCondition) {
        log.info("Begin migrate data from table "+table+" to Redis");

        if (null == whereCondition) {
            whereCondition = "";
        }
        if (whereCondition.length() > 0) {
            whereCondition = "WHERE "+whereCondition;
        }
        String sql = SQL.replace("{WHERE_CONDITION}", whereCondition)
                        .replace("{MYSQL_TABLE}", table);
        int totalCounter = 0;
        int failedCounter = 0;
        try (Connection conn = dataSource.getConnection();

             Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                   ResultSet.CONCUR_READ_ONLY)) {
            DatabaseMetaData metaData = conn.getMetaData();
            String[] columnNames;
            HashSet<String> primaryKeys = new HashSet<>();

            try (ResultSet rs = metaData.getColumns(null, "%", table, "%")) {
                LinkedList<String> columns = new LinkedList<>();
                while (rs.next()) {
                    columns.addLast(rs.getString("COLUMN_NAME").toLowerCase());
                }
                columnNames = columns.toArray(new String[columns.size()]);
            }

            try (ResultSet rs = metaData.getPrimaryKeys(null, null, table)) {
                while(rs.next()) {
                    primaryKeys.add(rs.getString("COLUMN_NAME").toLowerCase());
                }
            }

            stmt.setFetchSize(fetchSize);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    totalCounter++;
                    MysqlInsertRow event = new MysqlInsertRow();
                    event.table(table);
                    for (int i = 0; i < columnNames.length; i++) {
                        String value = rs.getString(i+1);
                        Column column = new Column(columnNames[i], value, Types.VARCHAR);
                        event.addColumn(column);
                        if (primaryKeys.contains(columnNames[i])) {
                            event.addPrimaryKey(column);
                        }
                    }
                    try {
                        redisReceiver.receive(event);
                    } catch (Exception e) {
                        log.error("Can not insert Redis", e);
                        failedCounter++;
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Select MySQL Error", e);
        }
        log.info(table+" total "+totalCounter+" rows, "+failedCounter+" rows failed");
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setRedisReceiver(RedisReceiver redisReceiver) {
        this.redisReceiver = redisReceiver;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    private final static Log log = LogFactory.getLog(Mysql2Redis.class);

    private final String SQL;

    private RedisReceiver redisReceiver;
    private DataSource dataSource;
    private int fetchSize = 100;
}
