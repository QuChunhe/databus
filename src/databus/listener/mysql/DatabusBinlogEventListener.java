package databus.listener.mysql;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;

import databus.event.mysql.AbstractMysqlWriteRow;

public class DatabusBinlogEventListener implements BinlogEventListener {

    public DatabusBinlogEventListener(MysqlListener listener) {
        this.listener = listener;
        preTableMapEvent = null;
    }

    @Override
    public void onEvents(BinlogEventV4 event) {
        switch (event.getHeader().getEventType()) {
        case MySQLConstants.TABLE_MAP_EVENT:
            setTableMapEvent((TableMapEvent) event);
            break;
        
        case MySQLConstants.WRITE_ROWS_EVENT_V2:  
        case MySQLConstants.WRITE_ROWS_EVENT:
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
        case MySQLConstants.UPDATE_ROWS_EVENT:
        case MySQLConstants.DELETE_ROWS_EVENT_V2:
        case MySQLConstants.DELETE_ROWS_EVENT:
            buildMySQLEvent((AbstractRowEvent) event);
            break;
        
        case MySQLConstants.ROTATE_EVENT:            
            updateMysqlListener((RotateEvent) event);
            break;
            
        default:

        }
    }
    
    private void updateMysqlListener(RotateEvent event) {        
        listener.setBinlog(event.getBinlogFileName().toString(),
                           event.getBinlogPosition());
    }

    private void buildMySQLEvent(AbstractRowEvent currentEvent) {
        listener.setNextPosition(currentEvent.getHeader().getNextPosition());
        
        String database = preTableMapEvent.getDatabaseName().toString().toLowerCase();
        String table = preTableMapEvent.getTableName().toString().toLowerCase();
        String fullName = database+"."+table;
        if (!listener.doesPermit(fullName)) {
            return;
        }        
        if (null == preTableMapEvent) {
            log.error("Previous TableMapEvent is null : "+currentEvent.toString());
            return;
        }
        if (preTableMapEvent.getTableId() != currentEvent.getTableId()) {
            log.error("Current event isn't consistend with TableMapEvent : "+
                    preTableMapEvent.toString()+" ; "+currentEvent.toString());
            return;
        }

        Set<String> primaryKeys = listener.getPrimaryKeys(fullName);
        ColumnAttribute[] attributes = listener.getTypes(fullName);
        String[] columns = listener.getColumns(fullName);
        MysqlWriteEventFactory factory;
        switch (currentEvent.getHeader().getEventType()) {
        case MySQLConstants.WRITE_ROWS_EVENT_V2:
            List<Row> wRows2 = ((WriteRowsEventV2)currentEvent).getRows();
            factory = new MysqlInsertEventFactory(columns, attributes, primaryKeys, wRows2);
            break;
            
        case MySQLConstants.WRITE_ROWS_EVENT:
            List<Row> wRows1 = ((WriteRowsEvent)currentEvent).getRows();
            factory = new MysqlInsertEventFactory(columns, attributes, primaryKeys, wRows1);
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            List<Pair<Row>> uRows2 = ((UpdateRowsEventV2)currentEvent).getRows(); 
            factory = new MysqlUpdateEventFactory(columns, attributes, primaryKeys, uRows2);
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT:
            List<Pair<Row>> uRows1 = ((UpdateRowsEvent)currentEvent).getRows(); 
            factory = new MysqlUpdateEventFactory(columns, attributes, primaryKeys, uRows1);
            break;
            
        case MySQLConstants.DELETE_ROWS_EVENT_V2:
            List<Row> dRows2 = ((DeleteRowsEventV2)currentEvent).getRows();
            factory = new MysqlDeleteEventFactory(columns, attributes, primaryKeys, dRows2);            
            break;
            
        case MySQLConstants.DELETE_ROWS_EVENT:
            List<Row> dRows1 = ((DeleteRowsEvent)currentEvent).getRows();
            factory = new MysqlDeleteEventFactory(columns, attributes, primaryKeys, dRows1);            
            break;
            
        default:
            log.error("Current BinlogEven is not a write event : "+currentEvent.toString());
            return;
        }
        
        long time = currentEvent.getHeader().getTimestamp();
        long serverId = preTableMapEvent.getHeader().getServerId();
        while(factory.hasMore()) {
            AbstractMysqlWriteRow event = factory.next();
            if (event.row().size() > 0) {
                  event.database(database)
                       .serverId(serverId)
                       .table(table)
                       .time(time);
                listener.onEvent(event);  
            }
        }
    }
    
    private void setTableMapEvent(TableMapEvent tableMapEvent) {
        preTableMapEvent = tableMapEvent;
    }

    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);
    
    private TableMapEvent preTableMapEvent;
    private MysqlListener listener;
    
}
