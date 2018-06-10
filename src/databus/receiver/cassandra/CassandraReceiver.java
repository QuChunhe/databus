package databus.receiver.cassandra;

import java.io.IOException;

import com.datastax.driver.core.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.util.FutureChecker;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public class CassandraReceiver implements Receiver {
    public CassandraReceiver() {
    }

    @Override
    public void close() throws IOException {
        if ((null!=cluster) && !cluster.isClosed()) {
            cluster.close();
        }
    }

    @Override
    public void receive(Event event) {
        try(CassandraConnection conn = new CassandraConnection(cluster.connect(), futureChecker)) {
            cassandraBean.execute(conn, event);
        } catch (Exception e) {
            log.error("Can not receive "+event.toString(), e);
        }
    }

    public void setCassandraBean(CassandraBean cassandraBean) {
        this.cassandraBean = cassandraBean;
    }

    public void setFutureChecker(FutureChecker futureChecker) {
        this.futureChecker = futureChecker;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    private final static Log log = LogFactory.getLog(CassandraReceiver.class);

    private Cluster cluster;
    private FutureChecker futureChecker;
    private CassandraBean cassandraBean;
}
