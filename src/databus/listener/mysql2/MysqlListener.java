package databus.listener.mysql2;

import java.io.IOException;
import java.util.*;

import databus.core.Event;
import databus.core.Publisher;
import databus.event.MysqlEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import databus.listener.RunnableListener;
import databus.util.Backup;

/**
 * Created by Qu Chunhe on 2018-03-14.
 */
public class MysqlListener extends RunnableListener {

    public MysqlListener(Publisher publisher) {
        super(publisher);
    }

    @Override
    public void onEvent(Event event) {
        if (null == topicMap) {
            super.onEvent(event);
        }
        MysqlEvent mysqlEven = (MysqlEvent) event;
        String fullName = mysqlEven.database()+"."+mysqlEven.table();
        String topic = topicMap.get(fullName);
        if (null != topic) {
            publisher.publish(topic, event);
        } else {
            super.onEvent(event);
        }
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    public void setBinlog(long binlogPosition) {
        saveBinlog(this.binlogFileName, binlogPosition);
    }

    public void setBinlog(String binlogFileName, long binlogPosition) {
        this.binlogFileName = binlogFileName;
        this.binlogPosition = binlogPosition;
        saveBinlog(binlogFileName, binlogPosition);
    }

    public void setTopicMap(Map<String, String> topicMap) {
        this.topicMap = topicMap;
    }

    public void setRecordingInterval(long recordingInterval) {
        this.recordingInterval = recordingInterval;
    }

    public void setReplicatedTables(Collection<String> replicatedTables) {
        this.replicatedTableSet = new HashSet<>(replicatedTables);
    }

    public void setDeniedOperations(Collection<MysqlEvent.Type> deniedOperations) {
        deniedOperationSet.addAll(deniedOperations);
    }

    protected void saveBinlog(String binlogFileName, long binlogPosition) {
        long currentTime = System.currentTimeMillis();
        if ((currentTime-prevRecordedTime)<recordingInterval) {
            return;
        }
        prevRecordedTime = currentTime;
        saveBinlog0(binlogFileName,binlogPosition);
    }

    protected void saveBinlog0(String binlogFileName, long binlogPosition) {
        Backup.instance()
              .store(getRecordedId(),
                     "mysql.binlogFileName", binlogFileName,
                     "mysql.position", Long.toString(binlogPosition));
    }

    @Override
    protected ListeningRunner createListeningRunner() {
        return new MysqlListeningRunner();
    }

    protected MysqlDataSource createDataSource() {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUser(username);
        ds.setPassword(password);
        ds.setServerName(hostname);
        ds.setPort(port);
        return ds;
    }

    protected Set<MysqlEvent.Type> getDeniedOperationSet() {
        return deniedOperationSet;
    }

    private String getRecordedId() {
        if (null == recordedId) {
            recordedId = "MysqlListener-" + hostname + "-" + port + "-" + serverId;
        }
        return  recordedId;
    }

    private final static Log log = LogFactory.getLog(MysqlListener.class);

    protected HashSet<String> replicatedTableSet;
    private final Set<MysqlEvent.Type> deniedOperationSet = new HashSet<>();

    private BinaryLogClient client;
    private String hostname = "127.0.0.1";
    private int port = 3306;
    private String schema = null;
    private String username;
    private String password;
    private int serverId = 100;
    private long binlogPosition;
    private String binlogFileName = null;

    private long prevRecordedTime = 0;
    private long recordingInterval = 1000*10;
    private String recordedId = null;

    private BinlogEventProcessor binlogEventProcessor;

    private Map<String, String> topicMap = null;

    private class MysqlListeningRunner extends ListeningRunner {

        @Override
        public void runOnce() throws Exception {
            super.runOnce();
            client.connect();
        }

        @Override
        public void initialize() {
            client = new BinaryLogClient(hostname, port, schema, username, password);
            client.setBlocking(true);
            client.setServerId(serverId);
            Map<String, String> backup = Backup.instance().restore(getRecordedId());
            if (null != backup) {
                log.info(backup.toString());
                String backupBinLogFileName =  backup.get("mysql.binlogFileName");
                String backupPositionValue = backup.get("mysql.position");
                log.info(backupBinLogFileName);
                log.info(backupPositionValue);
                if ((null!=backupBinLogFileName) && (null!=backupPositionValue)) {
                    long backupPosition = Long.parseUnsignedLong(backupPositionValue);
                    String[] parts = binlogFileName.split("\\.");
                    String[] backupParts = backupBinLogFileName.split("\\.");
                    long num = Long.parseUnsignedLong(parts[parts.length-1]);
                    long backNum = Long.parseUnsignedLong(backupParts[backupParts.length-1]);
                    if ((num<backNum) || (num==backNum && binlogPosition<backupPosition)) {
                        binlogFileName = backupBinLogFileName;
                        binlogPosition = backupPosition;
                    }
                }
            }

            client.setBinlogPosition(binlogPosition);
            client.setBinlogFilename(binlogFileName);

            binlogEventProcessor = new BinlogEventProcessor(MysqlListener.this, replicatedTableSet);
            client.registerEventListener(binlogEventProcessor);
        }

        @Override
        public void processFinally() {
            saveBinlog0(binlogFileName, binlogPosition);
        }

        @Override
        public void stop(Thread owner) {
            try {
                client.disconnect();

            } catch (IOException e) {
                log.error("Cannot close MysqlListener", e);
            }
        }

        @Override
        public void close() {
        }
    }
}
