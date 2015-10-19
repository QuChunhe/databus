package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.common.util.MySQLConstants;

import databus.event.MySQLEvent;



public class DatabusBinlogEventListener implements BinlogEventListener{
    
    public DatabusBinlogEventListener(MySQLListener listener) {
        this.listener = listener;
        prevBinlogEvent = null;
    }
    
    @Override
    public void onEvents(BinlogEventV4 event) {        
        int type = event.getHeader().getEventType();
        
        switch (type) {
        case MySQLConstants.TABLE_MAP_EVENT:
            prevBinlogEvent = event;
            break;
        case MySQLConstants.XID_EVENT:
            break;
        case MySQLConstants.WRITE_ROWS_EVENT_V2:
            
            break;
        case MySQLConstants.WRITE_ROWS_EVENT:
            log.warn("MySQLRowsEventV1 "+event.toString());
            break;
        case MySQLConstants.UPDATE_ROWS_EVENT:
            log.warn("MySQLUpdateEventV1 "+event.toString());
            break;
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            break;
        default:
            
        }
        
        
    }
    
    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);
    
    private MySQLListener listener;
    private BinlogEventV4 prevBinlogEvent;
    private MySQLEvent currentMySQLEvent;
}
