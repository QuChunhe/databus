package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.util.MySQLConstants;

import databus.event.MysqlEvent;
import databus.event.mysql.MysqlDeleteEventWrapper;
import databus.event.mysql.MysqlInsertEventWrapper;
import databus.event.mysql.MysqlUpdateEventWrapper;
import databus.event.mysql.MysqlWriteEventWrapper;

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
            setCurrentBinlogEvent(event);
            break;
         case MySQLConstants.WRITE_ROWS_EVENT:
            setCurrentBinlogEvent(event);
            log.warn("MySQLRowsEventV1 " + event.toString());
            break;
        case MySQLConstants.UPDATE_ROWS_EVENT:
            setCurrentBinlogEvent(event);
            log.warn("MySQLUpdateEventV1 " + event.toString());
            break;
        default:
            setCurrentBinlogEvent(event);
        }
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
        String databaseName = tableMapEvent.getDatabaseName().toString();
        String tableName = tableMapEvent.getTableName().toString();
        long time;
        
        switch (curBinlogEventType) {
        case MySQLConstants.WRITE_ROWS_EVENT_V2:
            WriteRowsEventV2 writeRowsEvent = (WriteRowsEventV2) curBinlogEvent;          
            time = writeRowsEvent.getHeader().getTimestamp();
            MysqlInsertEventWrapper insertEvent = new MysqlInsertEventWrapper(
                                                   serverId, 
                                                   databaseName, 
                                                   tableName);
            insertEvent.setRows(writeRowsEvent.getRows());
            insertEvent.setTime(time);
            listener.onEvent(insertEvent);
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            UpdateRowsEventV2 updateRowsEvent = (UpdateRowsEventV2) curBinlogEvent;
            time = updateRowsEvent.getHeader().getTimestamp();
            MysqlUpdateEventWrapper updateEvent = new MysqlUpdateEventWrapper(
                                                    serverId, 
                                                    databaseName, 
                                                    tableName);
            updateEvent.setRows(updateRowsEvent.getRows());
            updateEvent.setTime(time);
            listener.onEvent(updateEvent);            
            break;
        case MySQLConstants.DELETE_ROWS_EVENT_V2:
            DeleteRowsEventV2 deleteRowEvent = (DeleteRowsEventV2)curBinlogEvent;
            time = deleteRowEvent.getHeader().getTimestamp();
            MysqlDeleteEventWrapper deleteEvent = new MysqlDeleteEventWrapper(
                                                    serverId, 
                                                    databaseName, 
                                                    tableName);
            deleteEvent.setRows(deleteRowEvent.getRows());
            deleteEvent.setTime(time);
            listener.onEvent(deleteEvent);
            break;
        default:
            log.error("Current BinlogEven isnot Write event: "+
                      curBinlogEvent.toString());
        }
    
    }


    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);

    private MySQLListener listener;
    private BinlogEventV4 preBinlogEvent;
    private BinlogEventV4 curBinlogEvent;
}
