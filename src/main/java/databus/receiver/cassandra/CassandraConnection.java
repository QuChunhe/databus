package databus.receiver.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.Callback;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class CassandraConnection implements Closeable {
    public CassandraConnection(Session session) {
        this(session, null);
    }

    public CassandraConnection(Session session, Executor executor) {
        this.session = session;
        this.executor= executor;
    }

    protected void insertAsync(String sql, Callback callback) {
        ResultSetFuture future = session.executeAsync(sql);
        future.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    ResultSet resultSet = future.get();
                    if (null != callback) {
                        callback.onSuccess(resultSet);
                    }
                } catch (InterruptedException e) {
                    log.info("CQL execution has been interrupted", e);
                    if (null != callback) {
                        callback.onFailure(e);
                    }
                } catch (ExecutionException e) {
                    log.info("CQL can not execute", e);
                    if (null != callback) {
                        callback.onFailure(e);
                    }
                } catch (Exception e) {
                    log.info("CQL execution meet some errors", e);
                    if (null != callback) {
                        callback.onFailure(e);
                    }
                }

            }
        }, executor);
    }

    protected ResultSet insert(String sql) {
        return session.execute(sql);
    }

    @Override
    public void close() throws IOException {
        session.close();
    }

    private final static Log log = LogFactory.getLog(CassandraConnection.class);

    private final Session session;
    private final Executor executor;
}
