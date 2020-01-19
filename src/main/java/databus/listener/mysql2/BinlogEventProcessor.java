package databus.listener.mysql2;

import java.util.Collection;
import java.util.Set;

import databus.event.MysqlEvent;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2018-03-16.
 */
public class BinlogEventProcessor implements BinaryLogClient.EventListener{
    public BinlogEventProcessor(MysqlListener mysqlListener, Set<String> replicatedTableSet) {
        this.mysqlListener = mysqlListener;
        this.replicatedTableSet = replicatedTableSet;
        this.deniedOperationSet = mysqlListener.getDeniedOperationSet();
        schemaCache = new SchemaCache(mysqlListener.createDataSource(), replicatedTableSet);
    }

    @Override
    public void onEvent(Event event) {
        EventHeaderV4 eventHeader = event.getHeader();
        EventData eventData = event.getData();
        switch (eventHeader.getEventType()) {
            case ROTATE:
                processRotateEvent(eventHeader, (RotateEventData)eventData);
                break;

            case TABLE_MAP:
                processTableMapEvent(eventHeader, (TableMapEventData)eventData);
                break;

            case UPDATE_ROWS:
                if (!deniedOperationSet.contains(MysqlEvent.Type.UPDATE)) {
                    processUpdateRowsEvent(eventHeader, (UpdateRowsEventData)eventData);
                }
                break;

            case DELETE_ROWS:
                if (!deniedOperationSet.contains(MysqlEvent.Type.DELETE)) {
                    processDeleteRowsEvent(eventHeader, (DeleteRowsEventData)eventData);
                }
                break;

            case WRITE_ROWS:
                if (!deniedOperationSet.contains(MysqlEvent.Type.INSERT)) {
                    processInsertRowsEvent(eventHeader, (WriteRowsEventData)eventData);
                }
                break;

            case XID:

                break;

            default:
                return;

        }

        if (EventType.ROTATE != eventHeader.getEventType()) {
            mysqlListener.setBinlog(eventHeader.getNextPosition());
        }
    }

    private void processRotateEvent(EventHeaderV4 eventHeader, RotateEventData rotateEventData) {
        mysqlListener.setBinlog(rotateEventData.getBinlogFilename(),
                                 rotateEventData.getBinlogPosition());
    }

    private void processTableMapEvent(EventHeaderV4 eventHeader,
                                      TableMapEventData tableMapEventData) {
        this.preTableMapEventData = tableMapEventData;
    }

    private void processUpdateRowsEvent(EventHeaderV4 eventHeader,
                                        UpdateRowsEventData updateRowsEventData) {
        TableInfo tableInfo = getTableInfo(eventHeader.getServerId(),
                                           updateRowsEventData.getTableId(),
                                           eventHeader.getTimestamp()/1000);
        if (null == tableInfo) {
            return;
        }

        Collection<MysqlEvent> events = updateRowsProcessor.convert(schemaCache, tableInfo,
                                                                    updateRowsEventData.getRows());
        onEvents(events);
    }

    private void processDeleteRowsEvent(EventHeaderV4 eventHeader,
                                        DeleteRowsEventData deleteRowsEventData) {
        TableInfo tableInfo = getTableInfo(eventHeader.getServerId(),
                                           deleteRowsEventData.getTableId(),
                                           eventHeader.getTimestamp()/1000);
        if (null == tableInfo) {
            return;
        }

        Collection<MysqlEvent> events = deleteRowsProcessor.convert(schemaCache, tableInfo,
                                                                    deleteRowsEventData.getRows());
        onEvents(events);
    }

    private void processInsertRowsEvent(EventHeaderV4 eventHeader,
                                        WriteRowsEventData writeRowsEventData) {
        TableInfo tableInfo = getTableInfo(eventHeader.getServerId(),
                                           writeRowsEventData.getTableId(),
                                           eventHeader.getTimestamp()/1000);
        if (null == tableInfo) {
            return;
        }
        Collection<MysqlEvent> events = insertRowsProcessor.convert(schemaCache, tableInfo,
                                                                    writeRowsEventData.getRows());
        onEvents(events);
    }


    private TableInfo getTableInfo(long serverId, long tableId, long unixtime) {
        if ((null==preTableMapEventData) || (preTableMapEventData.getTableId()!=tableId)) {
            log.error("Table id does not matche for "+tableId);
            return null;
        }
        TableInfo tableInfo = new TableInfo(serverId,
                                            preTableMapEventData.getDatabase(),
                                            preTableMapEventData.getTable(),
                                            unixtime);
        if (!doesReplicate(tableInfo.getFullName())) {
            return null;
        }
        return tableInfo;
    }

    private void onEvents(Collection<MysqlEvent> events) {
        for (MysqlEvent e : events) {
            mysqlListener.onEvent(e);
        }
    }

    private boolean doesReplicate(String fullTableName) {
        return replicatedTableSet.contains(fullTableName);
    }

    private final static Log log = LogFactory.getLog(BinlogEventProcessor.class);

    private final MysqlListener mysqlListener;
    private final Set<String> replicatedTableSet;
    private final SchemaCache schemaCache;
    private final Set<MysqlEvent.Type> deniedOperationSet;

    private final UpdateRowsProcessor updateRowsProcessor = new UpdateRowsProcessor();
    private final DeleteRowsProcessor deleteRowsProcessor = new DeleteRowsProcessor();
    private final InsertRowsProcessor insertRowsProcessor = new InsertRowsProcessor();

    private TableMapEventData preTableMapEventData = null;
}
