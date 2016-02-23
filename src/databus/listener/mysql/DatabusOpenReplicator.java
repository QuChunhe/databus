package databus.listener.mysql;

import com.google.code.or.OpenReplicator;

public class DatabusOpenReplicator extends OpenReplicator {
    public DatabusOpenReplicator() {
        super();
    }

    public void setRunning(boolean flag) {
        running.lazySet(flag);
    }
}
