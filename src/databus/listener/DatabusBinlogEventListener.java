package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.util.MySQLConstants;

import databus.event.mysql.MysqlAbstractWriteRows;
import databus.event.mysql.MysqlDeleteRows;
import databus.event.mysql.MysqlInsertRows;
import databus.event.mysql.MysqlUpdateRows;

public class DatabusBinlogEventListener implements BinlogEventListener {

    public DatabusBinlogEventListener(MysqlListener listener) {
        this.listener = listener;
        currentEvent = null;
        previousEvent = null;
    }

    @Override
    public void onEvents(BinlogEventV4 event) {
        int type = event.getHeader().getEventType();
        switch (type) {
        case MySQLConstants.XID_EVENT:
            buildMySQLEvent(event);
            break;
        case MySQLConstants.QUERY_EVENT:
            buildMySQLEvent(event);
            break;
        case MySQLConstants.WRITE_ROWS_EVENT:
            log.warn("MySQLRowsEventV1 " + event.toString());
            break;
        case MySQLConstants.UPDATE_ROWS_EVENT:
            log.warn("MySQLUpdateEventV1 " + event.toString());
            break;
        default:
            log.info("Other Event " + event.toString());
        }
        setCurrentBinlogEvent(event);
    }

    private void setCurrentBinlogEvent(BinlogEventV4 currentBinlogEvent) {
        previousEvent = this.currentEvent;
        this.currentEvent = currentBinlogEvent;
    }

    public boolean isConsistent(BinlogEventV4 nextBinlogEvent) {        
        return true;
    }

    private void buildMySQLEvent(BinlogEventV4 nextEvent) {
        if (!isConsistent(nextEvent)) {
            log.error("tableId is not consistent");
            return;
        }
        if ((null==previousEvent) || (null==currentEvent)) {
            return;
        }
        
        int preBinlogEventType = previousEvent.getHeader().getEventType();        
        if (preBinlogEventType != MySQLConstants.TABLE_MAP_EVENT) {
            log.info("Previous BinLogEvnt is not TableMapEvent: "+
                      previousEvent.toString());
            return;
        }
       
        TableMapEvent tableMapEvent = (TableMapEvent) previousEvent;
        String dbName = tableMapEvent.getDatabaseName().toString();
        String tableName = tableMapEvent.getTableName().toString();
        String fullName = dbName.toLowerCase()+"."+tableName.toLowerCase();
        log.info(fullName+" : "+currentEvent.toString());
        if (!listener.doPermit(fullName)) {
            return;
        }
    
        MysqlAbstractWriteRows<?> newEvent;
        switch (currentEvent.getHeader().getEventType()) {
        case MySQLConstants.WRITE_ROWS_EVENT_V2:         
            newEvent = new MysqlInsertRows();
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            newEvent = new MysqlUpdateRows();           
            break;
            
        case MySQLConstants.DELETE_ROWS_EVENT_V2:
            newEvent = new MysqlDeleteRows();
            break;
            
        default:
            log.error("Current BinlogEven is not Write event: "+
                      currentEvent.toString());
            return;
        }
        
        long serverId = tableMapEvent.getHeader().getServerId();
        newEvent.columns(listener.getColumns(fullName))
                .columnTypes(listener.getTypes(fullName))
                .primaryKeys(listener.getPrimaryKeys(fullName))
                .setRows(currentEvent);
        newEvent.serverId(serverId)
                .table(tableName)
                .database(dbName)
                .time(currentEvent.getHeader().getTimestamp());
                
        listener.onEvent(newEvent);
    }

    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);

    private MysqlListener listener;
    private BinlogEventV4 previousEvent;
    private BinlogEventV4 currentEvent;
}
