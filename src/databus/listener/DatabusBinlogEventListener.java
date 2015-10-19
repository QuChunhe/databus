package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
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
        case MySQLConstants.TABLE_MAP_EVENT:
            setCurrentBinlogEvent(event);
            break;
        case MySQLConstants.XID_EVENT:
            buildMySQLEvent(event);
            setCurrentBinlogEvent(event);
            break;
        case MySQLConstants.WRITE_ROWS_EVENT_V2:
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
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            setCurrentBinlogEvent(event);
            break;
        default:
            setCurrentBinlogEvent(event);
        }

    }

    private void setCurrentBinlogEvent(BinlogEventV4 currentBinlogEvent) {
        preBinlogEvent = this.curBinlogEvent;
        this.curBinlogEvent = currentBinlogEvent;
    }

    private void buildMySQLEvent(BinlogEventV4 nextBinlogEvent) {
        
        int preBinlogEventType = preBinlogEvent.getHeader()
                                               .getEventType();        
        if (preBinlogEventType != MySQLConstants.TABLE_MAP_EVENT) {
            return;
        }
        
        int curBinlogEventType = curBinlogEvent.getHeader()
                                               .getEventType();
      
        if (preBinlogEventType == MySQLConstants.TABLE_MAP_EVENT) {
            TableMapEvent tableMapEvent = (TableMapEvent) preBinlogEvent;
            switch (curBinlogEventType) {
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
                WriteRowsEventV2 writeRowsEvent = (WriteRowsEventV2) curBinlogEvent;
                long serverId = tableMapEvent.getHeader().getServerId();
                String databaseName = tableMapEvent.getDatabaseName().toString();
                String tableName = tableMapEvent.getTableName().toString();
                MySQLInsertEventWrapper event = new MySQLInsertEventWrapper(
                                                       serverId, 
                                                       databaseName, 
                                                       tableName);
                event.setRows(writeRowsEvent.getRows());
                listener.onEvent(event);
                break;
            default:
                break;
            }
        }
    }


    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);

    private MySQLListener listener;
    private BinlogEventV4 preBinlogEvent;
    private BinlogEventV4 curBinlogEvent;
}
