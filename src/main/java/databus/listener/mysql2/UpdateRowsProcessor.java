package databus.listener.mysql2;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.MysqlEvent;
import databus.event.mysql.Column;
import databus.event.mysql.ColumnAttribute;
import databus.event.mysql.MysqlUpdateRow;

public class UpdateRowsProcessor extends WriteRowsProcessor {
    
    public UpdateRowsProcessor() {
    }

    public List<MysqlEvent> convert(SchemaCache schemaCache, TableInfo tableInfo,
                                    List<Map.Entry<Serializable[], Serializable[]>> rows) {
        List<MysqlEvent> eventList = new LinkedList<>();
        String fullTableName = tableInfo.getFullName();
        final String[] columns = schemaCache.getColumns(fullTableName);
        final ColumnAttribute[] attributes = schemaCache.getTypes(fullTableName);
        final Set<String> primaryKeysSet = schemaCache.getPrimaryKeys(fullTableName);
        for(Map.Entry<Serializable[], Serializable[]> r : rows) {
            Serializable[] before = r.getKey();
            Serializable[] after = r.getValue();
            if (before.length != after.length) {
                log.error("before length is not equal after length : before="+ Arrays.toString(before)+
                          " ; after ="+Arrays.toString(after));
                continue;
            }
            int columnsLength = before.length;
            if (schemaCache.hasChangedScheme(fullTableName, columnsLength)) {
                log.error(fullTableName+" scheme has change");
                continue;
            }
            MysqlUpdateRow event = new MysqlUpdateRow();
            event.table(tableInfo.getTable())
                 .database(tableInfo.getDatabase())
                 .serverId(tableInfo.getServerId())
                 .time(tableInfo.getTime());
            for (int i=0; i<columnsLength; i++) {
                Serializable beforeColumn = before[i];
                Serializable afterColumn = after[i];
                String name = columns[i];
                ColumnAttribute attribute = attributes[i];
                if ((beforeColumn!=afterColumn) && (null==beforeColumn || !beforeColumn.equals(afterColumn))) {
                    Column column = new Column(name, toString(afterColumn,attribute), attribute.type());
                    event.addColumn(column);
                }
                if (primaryKeysSet.contains(name)) {
                    Column column = new Column(name, toString(beforeColumn,attribute), attribute.type());
                    event.addPrimaryKey(column);
                }
            }
            eventList.add(event);
        }

        return eventList;
    }

    private final static Log log = LogFactory.getLog(UpdateRowsProcessor.class);
}
