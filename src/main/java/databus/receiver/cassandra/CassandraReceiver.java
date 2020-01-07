package databus.receiver.cassandra;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public class CassandraReceiver implements Receiver {
    public CassandraReceiver() {
    }

    @Override
    public void close() throws IOException {
        if (!executor.isShutdown()) {
            log.info("Waiting ExecutorService termination!");
            executor.shutdown();
            while (!executor.isTerminated()) {
                try {
                    if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                        log.info("ExecutorService has shut down!");
                    } else {
                        log.warn("Waiting ExecutorService shutdown!");
                    }
                } catch (InterruptedException e) {
                    log.error("ExecutorService has been interrupted", e);
                }
            }
        }
        if ((null!=cluster) && !cluster.isClosed()) {
            cluster.close();
        }
    }

    @Override
    public void receive(Event event) {
        try(CassandraConnection conn = new CassandraConnection(cluster.connect(), executor)) {
            cassandraBean.execute(conn, event);
        } catch (Exception e) {
            log.error("Can not receive "+event.toString(), e);
        }
    }

    public void setCassandraBean(CassandraBean cassandraBean) {
        this.cassandraBean = cassandraBean;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    private final static Log log = LogFactory.getLog(CassandraReceiver.class);

    private Cluster cluster;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private CassandraBean cassandraBean;
}
