package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.util.MySQLConstants;

import databus.event.mysql.MysqlAbstractWriteEvent;
import databus.event.mysql.MysqlDeleteEvent;
import databus.event.mysql.MysqlInsertEvent;
import databus.event.mysql.MysqlUpdateEvent;

public class DatabusBinlogEventListener implements BinlogEventListener {

    public DatabusBinlogEventListener(MysqlListener listener) {
        this.listener = listener;
        curBinlogEvent = null;
        preBinlogEvent = null;
    }

    @Override
    public void onEvents(BinlogEventV4 event) {
        int type = event.getHeader().getEventType();

        switch (type) {
        case MySQLConstants.XID_EVENT:
            buildMySQLEvent(event);
            break;
         case MySQLConstants.WRITE_ROWS_EVENT:
            log.warn("MySQLRowsEventV1 " + event.toString());
            break;
        case MySQLConstants.UPDATE_ROWS_EVENT:
            log.warn("MySQLUpdateEventV1 " + event.toString());
            break;
        default:

        }
        setCurrentBinlogEvent(event);
    }

    private void setCurrentBinlogEvent(BinlogEventV4 currentBinlogEvent) {
        preBinlogEvent = this.curBinlogEvent;
        this.curBinlogEvent = currentBinlogEvent;
    }

    public boolean isConsistent(BinlogEventV4 nextBinlogEvent) {        
        return true;
    }

    private void buildMySQLEvent(BinlogEventV4 nextBinlogEvent) {
        if (!isConsistent(nextBinlogEvent)) {
            log.error("tableId is not consistent");
            return;
        }
        
        int preBinlogEventType = preBinlogEvent.getHeader().getEventType();        
        if (preBinlogEventType != MySQLConstants.TABLE_MAP_EVENT) {
            log.error("Previous BinLogEvnt is not TableMapEvent: "+
                      preBinlogEvent.toString());
            return;
        }
       
        TableMapEvent tableMapEvent = (TableMapEvent) preBinlogEvent;
        String dbName = tableMapEvent.getDatabaseName().toString();
        String tableName = tableMapEvent.getTableName().toString();
        String fullName = dbName.toLowerCase()+"."+tableName.toLowerCase();
        if (!listener.doPermit(fullName)) {
            return;
        }
    
        MysqlAbstractWriteEvent<?> newEvent;
        switch (curBinlogEvent.getHeader().getEventType()) {
        case MySQLConstants.WRITE_ROWS_EVENT_V2:         
            newEvent = new MysqlInsertEvent();
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            newEvent = new MysqlUpdateEvent();           
            break;
            
        case MySQLConstants.DELETE_ROWS_EVENT_V2:
            newEvent = new MysqlDeleteEvent();
            break;
            
        default:
            log.error("Current BinlogEven is not Write event: "+
                      curBinlogEvent.toString());
            return;
        }
        
        long serverId = tableMapEvent.getHeader().getServerId();
        newEvent.columnNames(listener.getColumns(fullName))
                .columnTypes(listener.getTypes(fullName))
                .primaryKeys(listener.getPrimaryKeys(fullName))
                .setRows(curBinlogEvent);
        newEvent.serverId(serverId)
                .tableName(tableName)
                .databaseName(dbName)
                .time(curBinlogEvent.getHeader().getTimestamp());
                
        listener.onEvent(newEvent);
    }


    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);

    private MysqlListener listener;
    private BinlogEventV4 preBinlogEvent;
    private BinlogEventV4 curBinlogEvent;
}
