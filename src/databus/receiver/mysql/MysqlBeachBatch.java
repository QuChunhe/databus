package databus.receiver.mysql;

import java.sql.Connection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class MysqlBeachBatch implements MysqlBean {

    public void setMessageBeanMap(Map<String, MysqlBean> messageBeanMap) {
        this.messageBeanMap = messageBeanMap;
    }

    @Override
    public void execute(Connection connection, String key, String message) {
        MysqlBean messageBean = messageBeanMap.get(key);
        if (null == messageBean) {
            log.error("Can not get value bean for "+key);
            return;
        }
        messageBean.execute(connection, key, message);
    }

    private final static Log log = LogFactory.getLog(MysqlBeachBatch.class);

    private Map<String, MysqlBean> messageBeanMap;
}
