package databus.receiver.cassandra;

import com.datastax.driver.core.ResultSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.Callback;

/**
 * Created by Qu Chunhe on 2018-06-04.
 */
public class LogFailureCallback implements Callback<ResultSet> {
    public LogFailureCallback(String sql) {
        this.sql = sql;
    }

    @Override
    public void onFailure(Throwable t) {
        log.error("Can not execute "+sql, t);
    }

    @Override
    public void onSuccess(ResultSet rows) {
    }

    private final static Log log = LogFactory.getLog(LogFailureCallback.class);

    private final String sql;
}