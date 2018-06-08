package databus.receiver.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import databus.core.Event;
import databus.event.mysql.Column;
import databus.event.mysql.MysqlDeleteRow;
import databus.event.mysql.MysqlInsertRow;
import databus.event.mysql.MysqlUpdateRow;
import databus.receiver.mysql.MysqlHelper;
import databus.event.MysqlEvent;

/**
 * Created by Qu Chunhe on 2018-05-30.
 */
public class CassandraBean4Mysql implements CassandraBean {

    public CassandraBean4Mysql() {
    }

    public void setDoesDiscardUnspecifiedColumn(boolean doesDiscardUnspecifiedColumn) {
        this.doesDiscardUnspecifiedColumn = doesDiscardUnspecifiedColumn;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setColumnMap(Map<String, String> columnMap) {
        this.columnMap = columnMap;
    }

    @Override
    public void execute(CassandraConnection conn, Event event) {
        if (!checkCondition()) {
            return;
        }
        String sql = null;
        if (event instanceof MysqlInsertRow) {
            sql = toInsertSql((MysqlInsertRow) event);
        } else if (event instanceof MysqlUpdateRow) {
            sql = toUpdateSql((MysqlUpdateRow) event);
        } else if (event instanceof MysqlDeleteRow) {
            sql = toDeleteSql((MysqlDeleteRow) event);
        }

        if (null == sql) {
            log.error("Can not convert SQL from " + event.toString());
        } else {
            conn.insertAsync(sql, new LogFailureCallback(sql));
            log.info("Execute : "+sql);
        }
    }

    private String toCassandraTable(MysqlEvent event) {
        return null != table ? table : event.database()+"."+event.table();
    }

    private String toCassandraColumn(String mysqlColumn) {
        String cassandraColumn = columnMap.get(mysqlColumn);
        if (null != cassandraColumn) {
            return cassandraColumn;
        }
        if (!doesDiscardUnspecifiedColumn) {
            return mysqlColumn;
        }
        return null;
    }

    private String toInsertSql(MysqlInsertRow event) {
        StringBuilder sqlBuilder = new StringBuilder(128);
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(toCassandraTable(event));
        sqlBuilder.append(" (");
        StringBuilder valuesBuilder = new StringBuilder(64);
        valuesBuilder.append('(');
        List<Column> row = event.row();
        for(Column column : row) {
            String cassandraColumn = toCassandraColumn(column.name());
            if (null != cassandraColumn) {
                sqlBuilder.append(cassandraColumn);
                sqlBuilder.append(", ");
                appendValue(valuesBuilder, column);
                valuesBuilder.append(", ");
            }
        }
        sqlBuilder.setLength(sqlBuilder.length()-2);
        sqlBuilder.append(')');
        valuesBuilder.setLength(valuesBuilder.length()-2);
        valuesBuilder.append(')');
        sqlBuilder.append(" VALUES ");
        sqlBuilder.append(valuesBuilder);
        return sqlBuilder.toString();
    }

    private String toUpdateSql(MysqlUpdateRow event) {
        StringBuilder sqlBuilder = new StringBuilder(128);
        appendSetEqual(sqlBuilder, event.row());
        if (sqlBuilder.length() == 0) {
            return null;
        }

        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(toCassandraTable(event));
        sqlBuilder.append(" SET ");
        sqlBuilder.append(" WHERE ");
        appendWhereEqual(sqlBuilder, event.primaryKeys());
        return sqlBuilder.toString();
    }

    private String toDeleteSql(MysqlDeleteRow event) {
        StringBuilder sqlBuilder = new StringBuilder(64);
        sqlBuilder.append("DELETE FROM ");
        sqlBuilder.append(toCassandraTable(event));
        sqlBuilder.append(" WHERE ");
        appendWhereEqual(sqlBuilder, event.primaryKeys());
        return sqlBuilder.toString();
    }

    private void appendSetEqual(StringBuilder builder, List<Column> row) {
        appendEqual(builder, row,", ");
    }

    private void appendEqual(StringBuilder builder, List<Column> row, String seperator) {
        for(Column c : row) {
            String cassandraColumn = toCassandraColumn(c.name());
            if (null != cassandraColumn) {
                builder.append(cassandraColumn);
                builder.append('=');
                appendValue(builder, c);
                builder.append(seperator);
            }
        }
        builder.setLength(builder.length()-seperator.length());
    }

    private void appendWhereEqual(StringBuilder builder, List<Column> row) {
        appendEqual(builder, row, " AND ");
    }


    private void appendValue(StringBuilder builder, Column column) {
        if (column.doesUseQuotation()) {
            if (null == column.value()) {
                builder.append("NULL");
            } else {
                builder.append("'");
                builder.append(MysqlHelper.quoteReplacement(column.value()));
                builder.append("'");
            }
        } else {
            builder.append(column.value());
        }
    }

    private boolean checkCondition() {
        if ((columnMap.size()==0) && doesDiscardUnspecifiedColumn) {
            log.error("ColumnMap is null");
            return false;
        }
        return true;
    }


    private final static Log log = LogFactory.getLog(CassandraBean4Mysql.class);

    private String table = null;
    private Map<String, String> columnMap = new HashMap<>();
    private boolean doesDiscardUnspecifiedColumn = false;
}
