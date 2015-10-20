package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.util.MySQLConstants;

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
            return;
        }
        
        int preBinlogEventType = preBinlogEvent.getHeader()
                                               .getEventType();        
        if (preBinlogEventType != MySQLConstants.TABLE_MAP_EVENT) {
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
            MySQLInsertEventWrapper insertEvent = new MySQLInsertEventWrapper(
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
            MySQLUpdateEventWrapper updateEvent = new MySQLUpdateEventWrapper(
                                                    serverId, 
                                                    databaseName, 
                                                    tableName);
            updateEvent.setRows(updateRowsEvent.getRows());
            updateEvent.setTime(time);
            listener.onEvent(updateEvent);            
            break;
        default:
            
            break;
        }
    
    }


    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);

    private MySQLListener listener;
    private BinlogEventV4 preBinlogEvent;
    private BinlogEventV4 curBinlogEvent;
}
