package databus.listener.mysql;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;

import databus.event.mysql.AbstractMysqlWriteRow;


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
        default:
            
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
        String database = tableMapEvent.getDatabaseName().toString().toLowerCase();
        String table = tableMapEvent.getTableName().toString().toLowerCase();
        String fullName = database+"."+table;

        if (!listener.doPermit(fullName)) {
            return;
        }
    
        List<String> primaryKeys = listener.getPrimaryKeys(fullName);
        Set<String> primaryKeysSet = new HashSet<String>(primaryKeys);
        List<Integer> types = listener.getTypes(fullName);
        List<String> columns = listener.getColumns(fullName);
        MysqlWriteEventFactory factory;
        switch (currentEvent.getHeader().getEventType()) {
        case MySQLConstants.WRITE_ROWS_EVENT_V2:
            List<Row> wRows2 = ((WriteRowsEventV2)currentEvent).getRows();
            factory = new MysqlInsertEventFactory(columns, types, primaryKeysSet, wRows2);
            break;
            
        case MySQLConstants.WRITE_ROWS_EVENT:
            List<Row> wRows1 = ((WriteRowsEvent)currentEvent).getRows();
            factory = new MysqlInsertEventFactory(columns, types, primaryKeysSet, wRows1);
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            List<Pair<Row>> uRows2 = ((UpdateRowsEventV2)currentEvent).getRows(); 
            factory = new MysqlUpdateEventFactory(columns, types, primaryKeysSet, uRows2);
            break;
            
        case MySQLConstants.UPDATE_ROWS_EVENT:
            List<Pair<Row>> uRows1 = ((UpdateRowsEvent)currentEvent).getRows(); 
            factory = new MysqlUpdateEventFactory(columns, types, primaryKeysSet, uRows1);
            break;
            
        case MySQLConstants.DELETE_ROWS_EVENT_V2:
            List<Row> dRows2 = ((DeleteRowsEventV2)currentEvent).getRows();
            factory = new MysqlDeleteEventFactory(columns, types, primaryKeysSet, dRows2);            
            break;
            
        case MySQLConstants.DELETE_ROWS_EVENT:
            List<Row> dRows1 = ((DeleteRowsEvent)currentEvent).getRows();
            factory = new MysqlDeleteEventFactory(columns, types, primaryKeysSet, dRows1);            
            break;
            
        default:
            log.error("Current BinlogEven is not a write event : "+
                      currentEvent.toString());
            return;
        }
        
        long time = currentEvent.getHeader().getTimestamp();
        long serverId = tableMapEvent.getHeader().getServerId();
        while(factory.hasMore()) {
            AbstractMysqlWriteRow event = factory.next();
            event.database(database)
                 .serverId(serverId)
                 .table(table)
                 .time(time);
            listener.onEvent(event);
        }
    }    

    private static Log log = LogFactory.getLog(DatabusBinlogEventListener.class);

    private MysqlListener listener;
    private BinlogEventV4 previousEvent;
    private BinlogEventV4 currentEvent;
    
}
