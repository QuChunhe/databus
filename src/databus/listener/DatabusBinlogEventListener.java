package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.util.MySQLConstants;

import databus.event.MysqlWriteEvent;
import databus.event.mysql.MysqlDeleteEvent;
import databus.event.mysql.MysqlInsertEvent;
import databus.event.mysql.MysqlUpdateEvent;

public class DatabusBinlogEventListener implements BinlogEventListener {

    public DatabusBinlogEventListener(MySQLListener listener) {
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
        
        int curBinlogEventType = curBinlogEvent.getHeader().getEventType();        
        TableMapEvent tableMapEvent = (TableMapEvent) preBinlogEvent;
        long serverId = tableMapEvent.getHeader().getServerId();
        String dbName = tableMapEvent.getDatabaseName().toString();
        String tableName = tableMapEvent.getTableName().toString();
        
        @SuppressWarnings("rawtypes")
        MysqlWriteEvent newEvent;
        switch (curBinlogEventType) {
        case MySQLConstants.WRITE_ROWS_EVENT_V2:         
            newEvent = new MysqlInsertEvent(serverId, dbName, tableName);
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            newEvent = new MysqlUpdateEvent(serverId, dbName, tableName);           
            break;
            
        case MySQLConstants.DELETE_ROWS_EVENT_V2:
            newEvent = new MysqlDeleteEvent(serverId, dbName, tableName);
            break;
            
        default:
            log.error("Current BinlogEven is not Write event: "+
                      curBinlogEvent.toString());
            return;
        }
        newEvent.setRow(curBinlogEvent);
        newEvent.time(curBinlogEvent.getHeader().getTimestamp());
        listener.onEvent(newEvent);
    }


    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);

    private MySQLListener listener;
    private BinlogEventV4 preBinlogEvent;
    private BinlogEventV4 curBinlogEvent;
}
