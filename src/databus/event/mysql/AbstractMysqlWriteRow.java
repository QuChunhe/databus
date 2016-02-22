package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractMysqlWriteRow  extends AbstractMysqlEvent 
                                             implements MysqlWriteRow{
    
    public AbstractMysqlWriteRow() {
        super();
        row = new LinkedList<Column>();
        primaryKeys = new LinkedList<Column>();
    }

    @Override
    public List<Column> row() {
        return row;
    }

    @Override
    public List<Column> primaryKeys() {
        return primaryKeys;
    }
    
    public AbstractMysqlWriteRow addPrimaryKey(Column primaryKey) {
        primaryKeys.add(primaryKey);
        return this;
    }
    
    public AbstractMysqlWriteRow addColumn(Column column) {
        row.add(column);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(256);
        builder.append(source())
               .append(":")
               .append(type())
               .append(" = ")
               .append("{")
               .append("\"time\": ")
               .append(time())
               .append(", ")
               .append("\"ipAddress\": \"")
               .append(ipAddress().getHostAddress())
               .append("\", ")
               .append("\"source\": \"")
               .append(source())
               .append("\", ")
               .append("\"serverId\": ")
               .append(serverId())
               .append(", ")
               .append("\"database\": \"")
               .append(database())
               .append("\", ")
               .append("\"table\": \"")
               .append(table())
               .append("\", ")
               .append("\"primaryKeys\": [");
        for(Column c: primaryKeys) {
            builder.append(c.toString())
                   .append(", ");
        }
        builder.delete(builder.length()-2, builder.length())
               .append("], ")
               .append("row: [");
        for(Column c: row) {
            builder.append(c.toString())
                   .append(", ");
        }
        builder.delete(builder.length()-2, builder.length())
               .append("]")
               .append("}");
        return builder.toString();
    }    

    @Override
    public void clear() {
        super.clear();
        row.clear();
        primaryKeys.clear();
        row = null;
        primaryKeys = null;
    }

    private List<Column> row;
    private List<Column> primaryKeys;
}
