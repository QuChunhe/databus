package databus.listener.mysql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.or.OpenReplicator;

import databus.core.Restartable;

public class DatabusOpenReplicator extends OpenReplicator implements Restartable{
    public DatabusOpenReplicator() {
        super();
    }
    
    public boolean isRunning() {
        return (binlogParser!=null) && binlogParser.isRunning();
    }

    @Override
    public void restart() {
        running.lazySet(false);
        setTransport(null);
        setBinlogParser(null);
        try {
            start();
        } catch (Exception e) {
            log.error("OpenRelicator throws a Exception",e);
        }
    }
    
    private static Log log = LogFactory.getLog(DatabusOpenReplicator.class);
}
