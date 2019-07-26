package databus.util;

import java.util.Properties;

import com.datastax.driver.core.*;

/**
 * Created by Qu Chunhe on 2018-06-10.
 */
public class CassandraClusterBuilder {

    public static Cluster build(String configFile) {
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

        SocketOptions socketOptions = new SocketOptions();

        String connectTimeoutMillis = properties.getProperty("connectTimeoutMillis", "60000");
        socketOptions.setConnectTimeoutMillis(parseInt(connectTimeoutMillis));

        String readTimeoutMillis = properties.getProperty("readTimeoutMillis", "40000");
        socketOptions.setReadTimeoutMillis(parseInt(readTimeoutMillis));

        socketOptions.setTcpNoDelay(true);

        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        if ((null!=username) && (null!=password)) {        	
        	return Cluster.builder()
                    .addContactPoints(contactPoints)
                    .withPoolingOptions(poolingOptions)
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
                    .withSocketOptions(socketOptions)
                    .withCredentials(username.trim(), password.trim())
                    .build();
        }
        return Cluster.builder()
                      .addContactPoints(contactPoints)
                      .withPoolingOptions(poolingOptions)
                      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
                      .withSocketOptions(socketOptions)
                      .build();
    }

    private static int parseInt(String value) {
        return Integer.parseUnsignedInt(value);
    }
}
