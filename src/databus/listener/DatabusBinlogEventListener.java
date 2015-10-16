package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.common.util.MySQLConstants;

public class DatabusBinlogEventListener implements BinlogEventListener{    
    
    public DatabusBinlogEventListener(MySQLListener listener) {
        this.listener = listener;
    }
    
    @Override
    public void onEvents(BinlogEventV4 event) {        
        int type = event.getHeader().getEventType();
        
        switch (type) {
        case MySQLConstants.TABLE_MAP_EVENT:
            break;
        case MySQLConstants.XID_EVENT:
            break;
        case MySQLConstants.WRITE_ROWS_EVENT_V2:
            break;
        case MySQLConstants.WRITE_ROWS_EVENT:
            break;
        case MySQLConstants.UPDATE_ROWS_EVENT:
            break;
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            break;
        default:
            
        }
        
        
    }
    
    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);
    
    private MySQLListener listener;

}
