package databus.receiver.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import databus.util.Callback;
import databus.util.FutureChecker;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class CassandraConnection implements Closeable {
    public CassandraConnection(Session session) {
        this(session, null);
    }

    public CassandraConnection(Session session, FutureChecker futureChecker) {
        this.session = session;
        this.futureChecker = futureChecker;
    }

    protected void insertAsync(String sql, Callback callback) {
        ResultSetFuture future = session.executeAsync(sql);
        if ((null != callback) && (null != futureChecker)) {
            futureChecker.check(future, callback);
        }
    }

    protected ResultSet insert(String sql) throws ExecutionException,
                                                  InterruptedException {
        return session.executeAsync(sql).get();
    }

    @Override
    public void close() throws IOException {
        session.close();
    }

    private final Session session;
    private final FutureChecker futureChecker;
}
