package databus.receiver.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-06-04.
 */
public abstract class BatchCassandraBean implements CassandraBean {
    public BatchCassandraBean() {
    }

    public void setCassandraBeanMap(Map<String, CassandraBean> cassandraBeanMap) {
        this.cassandraBeanMap.putAll(cassandraBeanMap);
    }

    @Override
    public void execute(CassandraConnection conn, Event event) {
        String key = toKey(event);
        if (null == key) {
            log.error("Can not get key from "+event.toString());
            return;
        }
        CassandraBean cassandraBean = cassandraBeanMap.get(key);
        if (null == cassandraBean) {
            log.error("Can not find CassandraBean for "+event.toString());
            return;
        }
        cassandraBean.execute(conn, event);
    }

    protected abstract String toKey(Event event);

    private final static Log log = LogFactory.getLog(BatchCassandraBean.class);

    private Map<String, CassandraBean> cassandraBeanMap = new HashMap<>();
}
