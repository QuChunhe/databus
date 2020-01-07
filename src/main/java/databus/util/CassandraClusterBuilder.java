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

        String localCoreConnectionsPerHost = properties.getProperty("localCoreConnectionsPerHost", "2");
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,
                                                 parseInt(localCoreConnectionsPerHost));

        String remoteCoreConnectionsPerHost = properties.getProperty("remoteCoreConnectionsPerHost");
        if (null != remoteCoreConnectionsPerHost) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE,
                                                     parseInt(remoteCoreConnectionsPerHost));
        }

        String localMaxConnectionsPerHost = properties.getProperty("localMaxConnectionsPerHost", "4");
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,
                                                parseInt(localMaxConnectionsPerHost));

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

        String localMaxRequestsPerConnection = properties.getProperty("localMaxRequestsPerConnection","64");
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL,
                                                   parseInt(localMaxRequestsPerConnection));

        String remoteMaxRequestsPerConnection = properties.getProperty("remoteMaxRequestsPerConnection","64");
        poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE,
                                                   parseInt(remoteMaxRequestsPerConnection));


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

        String consistencyLevelValue = properties.getProperty("consistencyLevel", "ONE");
        ConsistencyLevel consistencyLevel;
        switch (consistencyLevelValue.toUpperCase()) {
            case "ANY":
                consistencyLevel = ConsistencyLevel.ANY;
                break;
            case "ONE":
                consistencyLevel = ConsistencyLevel.ONE;
                break;
            case "TWO":
                consistencyLevel = ConsistencyLevel.TWO;
                break;
            case "THREE":
                consistencyLevel = ConsistencyLevel.THREE;
                break;
            case "QUORUM":
                consistencyLevel = ConsistencyLevel.QUORUM;
                break;
            case "ALL":
                consistencyLevel = ConsistencyLevel.ALL;
                break;
            case "LOCAL_QUORUM":
                consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
                break;
            case "EACH_QUORUM":
                consistencyLevel = ConsistencyLevel.EACH_QUORUM;
                break;
            case "SERIAL":
                consistencyLevel = ConsistencyLevel.SERIAL;
                break;
            case "LOCAL_SERIAL":
                consistencyLevel = ConsistencyLevel.LOCAL_SERIAL;
                break;
            case "LOCAL_ONE":
            default:
                consistencyLevel = ConsistencyLevel.LOCAL_ONE;

        }
        QueryOptions queryOptions=new QueryOptions();
        queryOptions.setConsistencyLevel(consistencyLevel);

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
                          .withQueryOptions(queryOptions)
                          .withCompression(ProtocolOptions.Compression.LZ4)
                          .addContactPoints(contactPoints)
                          .withPoolingOptions(poolingOptions)
                          .withQueryOptions(queryOptions)
                          .withSocketOptions(socketOptions)
                          .withCredentials(username.trim(), password.trim())
                          .build();
        }
        return Cluster.builder()
                      .withQueryOptions(queryOptions)
                      .withCompression(ProtocolOptions.Compression.LZ4)
                      .addContactPoints(contactPoints)
                      .withPoolingOptions(poolingOptions)
                      .withQueryOptions(queryOptions)
                      .withSocketOptions(socketOptions)
                      .build();
    }

    private static int parseInt(String value) {
        return Integer.parseUnsignedInt(value);
    }
}
