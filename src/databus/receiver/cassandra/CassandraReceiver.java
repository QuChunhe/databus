package databus.receiver.cassandra;

import java.io.IOException;
import java.util.Properties;

import com.datastax.driver.core.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.util.FutureChecker;
import databus.util.Helper;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public abstract class CassandraReceiver implements Receiver {

    @Override
    public void close() throws IOException {
        if ((null!=cluster) && !cluster.isClosed()) {
            cluster.close();
        }
    }

    @Override
    public void receive(Event event) {
        try(CassandraConnection conn = new CassandraConnection(cluster.connect(), futureChecker)) {
            receive(conn, event);
        } catch (Exception e) {
            log.error("Can not receive "+event.toString(), e);
        }
    }


    public void setConfigFile(String configFile) {
        Properties properties = Helper.loadProperties(configFile);

        String contactPointsValue = properties.getProperty("contactPoints");
        if ((null==contactPointsValue) || contactPointsValue.length()==0) {
            throw new NullPointerException("contactPoints must have value!");
        }
        String[] contactPoints = contactPointsValue.split(",");
        for(int i=0; i<contactPoints.length; i++) {
            contactPoints[i] = contactPoints[i].trim();
        }

        PoolingOptions poolingOptions = new PoolingOptions();

        String localCoreConnectionsPerHost = properties.getProperty("localCoreConnectionsPerHost");
        if (null != localCoreConnectionsPerHost) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,
                                                     parseInt(localCoreConnectionsPerHost));
        }
        String remoteCoreConnectionsPerHost = properties.getProperty("remoteCoreConnectionsPerHost");
        if (null != remoteCoreConnectionsPerHost) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE,
                                                     parseInt(remoteCoreConnectionsPerHost));
        }

        String localMaxConnectionsPerHost = properties.getProperty("localMaxConnectionsPerHost");
        if (null != localMaxConnectionsPerHost) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,
                                                    parseInt(localMaxConnectionsPerHost));
        }
        String remoteMaxConnectionsPerHost = properties.getProperty("remoteMaxConnectionsPerHost");
        if (null != remoteMaxConnectionsPerHost) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE,
                                                    parseInt(remoteMaxConnectionsPerHost));
        }

        String localNewConnectionThreshold = properties.getProperty("localNewConnectionThreshold");
        if (null != localNewConnectionThreshold) {
            poolingOptions.setNewConnectionThreshold(HostDistance.LOCAL,
                                                     parseInt(localNewConnectionThreshold));
        }
        String remoteNewConnectionThreshold = properties.getProperty("remoteNewConnectionThreshold");
        if (null != remoteNewConnectionThreshold) {
            poolingOptions.setNewConnectionThreshold(HostDistance.REMOTE,
                                                     parseInt(remoteNewConnectionThreshold));
        }

        String localMaxRequestsPerConnection = properties.getProperty("localMaxRequestsPerConnection");
        if (null != localMaxRequestsPerConnection) {
            poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL,
                                                       parseInt(localMaxRequestsPerConnection));
        }
        String remoteMaxRequestsPerConnection = properties.getProperty("remoteMaxRequestsPerConnection");
        if (null != remoteMaxRequestsPerConnection) {
            poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE,
                                                       parseInt(remoteMaxRequestsPerConnection));
        }

        String idleTimeoutSeconds = properties.getProperty("idleTimeoutSeconds");
        if (null != idleTimeoutSeconds) {
            poolingOptions.setIdleTimeoutSeconds(parseInt(idleTimeoutSeconds));
        }

        String maxQueueSize = properties.getProperty("maxQueueSize");
        if (null != maxQueueSize) {
            poolingOptions.setMaxQueueSize(parseInt(maxQueueSize));
        }

        String heartbeatIntervalSeconds = properties.getProperty("heartbeatIntervalSeconds");
        if (null != heartbeatIntervalSeconds) {
            poolingOptions.setHeartbeatIntervalSeconds(parseInt(heartbeatIntervalSeconds));
        }

        cluster = Cluster.builder()
                         .addContactPoints(contactPoints)
                         .withPoolingOptions(poolingOptions)
                         .build();
    }


    public void setFutureChecker(FutureChecker futureChecker) {
        this.futureChecker = futureChecker;
    }

    protected abstract void receive(CassandraConnection conn, Event event);

    private int parseInt(String value) {
        return Integer.parseUnsignedInt(value);
    }

    private final static Log log = LogFactory.getLog(CassandraReceiver.class);

    private Cluster cluster;
    private FutureChecker futureChecker;
}
