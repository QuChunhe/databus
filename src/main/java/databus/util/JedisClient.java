package databus.util;

import java.net.URI;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import redis.clients.jedis.*;

/**
 * Created by Qu Chunhe on 2019-09-24.
 */
public class JedisClient extends Jedis implements RedisClient {

    public JedisClient() {
    }

    public JedisClient(String host) {
        super(host);
    }

    public JedisClient(HostAndPort hp) {
        super(hp);
    }

    public JedisClient(String host, int port) {
        super(host, port);
    }

    public JedisClient(String host, int port, boolean ssl) {
        super(host, port, ssl);
    }

    public JedisClient(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory,
                       SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public JedisClient(String host, int port, int timeout) {
        super(host, port, timeout);
    }

    public JedisClient(String host, int port, int timeout, boolean ssl) {
        super(host, port, timeout, ssl);
    }

    public JedisClient(String host, int port, int timeout, boolean ssl,
                       SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                       HostnameVerifier hostnameVerifier) {
        super(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public JedisClient(String host, int port, int connectionTimeout, int soTimeout) {
        super(host, port, connectionTimeout, soTimeout);
    }

    public JedisClient(String host, int port, int connectionTimeout, int soTimeout, boolean ssl) {
        super(host, port, connectionTimeout, soTimeout, ssl);
    }

    public JedisClient(String host, int port, int connectionTimeout, int soTimeout, boolean ssl,
                       SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                       HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory,
              sslParameters, hostnameVerifier);
    }

    public JedisClient(JedisShardInfo shardInfo) {
        super(shardInfo);
    }

    public JedisClient(URI uri) {
        super(uri);
    }

    public JedisClient(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                       HostnameVerifier hostnameVerifier) {
        super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public JedisClient(URI uri, int timeout) {
        super(uri, timeout);
    }

    public JedisClient(URI uri, int timeout, SSLSocketFactory sslSocketFactory,
                       SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public JedisClient(URI uri, int connectionTimeout, int soTimeout) {
        super(uri, connectionTimeout, soTimeout);
    }

    public JedisClient(URI uri, int connectionTimeout, int soTimeout,
                       SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                       HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    @Override
    public boolean doesSupportTransaction() {
        return true;
    }

}
