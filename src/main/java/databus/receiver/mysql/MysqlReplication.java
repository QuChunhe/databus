package databus.receiver.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.mysql.MysqlDeleteRow;
import databus.event.mysql.MysqlInsertRow;
import databus.event.mysql.MysqlUpdateRow;
import databus.event.mysql.Column;

public class MysqlReplication extends MysqlReceiver {

    public MysqlReplication() {
        super();
    }

    @Override
    protected void execute(Connection conn, Event event) {
        String sql = null;
        if (event instanceof MysqlInsertRow) {
            sql = getInsertSql((MysqlInsertRow) event);
        } else if (event instanceof MysqlUpdateRow) {
            sql = getUpdateSql((MysqlUpdateRow)event);
        } else if (event instanceof MysqlDeleteRow) {
            sql = getDeleteSql((MysqlDeleteRow)event);
        }

        if (null == sql) {
            log.error("Can not convert SQL from " + event.toString());
        } else if (executeWrite(conn, sql) < 1) {
            log.info("Execute : "+sql);
        }
    }

    protected String getPrefixInsertSql() {
        return "INSERT INTO ";
    }

    private String getInsertSql(MysqlInsertRow event) {
        StringBuilder sqlBuilder = new StringBuilder(128);
        sqlBuilder.append(getPrefixInsertSql());
        sqlBuilder.append(event.table());
        sqlBuilder.append(" (");
        StringBuilder valuesBuilder = new StringBuilder(64);
        valuesBuilder.append('(');
        List<Column> row = event.row();
        for(Column column : row) {          
            sqlBuilder.append(column.name());
            sqlBuilder.append(", ");
            appendValue(valuesBuilder, column);
            valuesBuilder.append(", ");
        }
        sqlBuilder.setLength(sqlBuilder.length()-2);
        sqlBuilder.append(')');
        valuesBuilder.setLength(valuesBuilder.length()-2);
        valuesBuilder.append(')');        
        sqlBuilder.append(" VALUES ");
        sqlBuilder.append(valuesBuilder);
        return sqlBuilder.toString();
    }
    
    private String getUpdateSql(MysqlUpdateRow event) {
        StringBuilder sqlBuilder = new StringBuilder(128);
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(event.table());
        sqlBuilder.append(" SET ");
        appendSetEqual(sqlBuilder, event.row());
        sqlBuilder.append(" WHERE ");
        appendWhereEqual(sqlBuilder, event.primaryKeys());
        return sqlBuilder.toString();
    }
    
    private String getDeleteSql(MysqlDeleteRow event) {
        StringBuilder sqlBuilder = new StringBuilder(64);
        sqlBuilder.append("DELETE FROM ");
        sqlBuilder.append(event.table());
        sqlBuilder.append(" WHERE ");
        appendWhereEqual(sqlBuilder, event.primaryKeys());  
        return sqlBuilder.toString();
    }
        
    private void appendSetEqual(StringBuilder builder, List<Column> row) {
        appendEqual(builder, row,", ");
    }
    
    private void appendEqual(StringBuilder builder, List<Column> row, String seperator) {
        for(Column column : row) {
            builder.append(column.name());
            builder.append('=');
            appendValue(builder, column);
            builder.append(seperator);
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
    
    private int executeWrite(Connection conn, String sql) {
        int count = -1;
        try (Statement stmt = conn.createStatement()) {
            stmt.setEscapeProcessing(true);
            count = stmt.executeUpdate(sql);
            if (count < 1) {
                log.error("Fail to insert into DB : "+sql);
            }
        } catch (SQLException e) {
            log.error("Can not execute : "+sql, e);
        }
        return count;
    }

    private final static Log log = LogFactory.getLog(MysqlReplication.class);
}
