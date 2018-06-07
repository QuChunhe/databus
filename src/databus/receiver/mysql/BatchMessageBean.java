package databus.receiver.mysql;

import java.sql.Connection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class BatchMessageBean implements MessageBean {

    public void setMessageBeanMap(Map<String, MessageBean> messageBeanMap) {
        this.messageBeanMap = messageBeanMap;
    }

    @Override
    public void execute(Connection connection, String key, String message) {
        MessageBean messageBean = messageBeanMap.get(key);
        if (null == messageBean) {
            log.error("Can not get value bean for "+key);
            return;
        }
        messageBean.execute(connection, key, message);
    }

    private final static Log log = LogFactory.getLog(BatchMessageBean.class);

    private Map<String, MessageBean> messageBeanMap;
}
