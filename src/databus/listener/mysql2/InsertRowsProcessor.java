package databus.listener.mysql2;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import databus.event.MysqlEvent;
import databus.event.mysql.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InsertRowsProcessor extends WriteRowsProcessor {
    
    public InsertRowsProcessor() {
    }

    public List<MysqlEvent> convert(SchemaCache schemaCache, TableInfo tableInfo,
                                    List<Serializable[]> rows) {

        String fullTableName = tableInfo.getFullName();
        List<MysqlEvent> eventList = new LinkedList<>();
        final String[] columns = schemaCache.getColumns(fullTableName);
        final ColumnAttribute[] attributes = schemaCache.getTypes(fullTableName);
        final Set<String> primaryKeysSet = schemaCache.getPrimaryKeys(fullTableName);
        for(Serializable[] r : rows) {
            AbstractMysqlWriteRow event = newInstance();
            event.table(tableInfo.getTable())
                 .database(tableInfo.getDatabase())
                 .serverId(tableInfo.getServerId())
                 .time(tableInfo.getTime());
            if (schemaCache.hasChangedScheme(fullTableName, r.length)) {
                log.error(fullTableName+" scheme has change");
                continue;
            }
            for(int i=0; i<r.length; i++) {
                Serializable value = r[i];
                String name = columns[i];
                ColumnAttribute attribute = attributes[i];
                Column column = new Column(name, toString(value,attribute.type()), attribute.type());
                event.addColumn(column);
                if (primaryKeysSet.contains(name)) {
                    event.addPrimaryKey(column);
                }
            }
            eventList.add(event);
        }

        return eventList;
    }

    protected AbstractMysqlWriteRow newInstance() {
        return new MysqlInsertRow();
    }

    private final static Log log = LogFactory.getLog(InsertRowsProcessor.class);

}
