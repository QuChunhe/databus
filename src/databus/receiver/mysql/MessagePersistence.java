package databus.receiver.mysql;

import java.sql.Connection;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.event.redis.RedisMessaging;

public class MessagePersistence extends MysqlReceiver{
    
    public MessagePersistence() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
        beanClassMap = new HashMap<>();
    }

    public void setBeanContext(BeanContext beanContext) {
        this.beanContext = beanContext;
    }

    public void setExecutableBeanClassMap(Map<String, String> executableBeanClassMap) {
        for (Map.Entry entry : executableBeanClassMap.entrySet()) {
            String key = entry.getKey().toString();
            String className = entry.getValue().toString();
            try {
                Class<? extends ExecutableBean> beanClass =
                                        (Class<? extends ExecutableBean>) Class.forName(className);
                beanClassMap.put(key.trim(), beanClass);
            } catch (ClassNotFoundException e) {
                log.error("Can not find "+className+"!", e);
                System.exit(1);
            }
        }
    }

    @Override
    protected String execute(Connection conn, Event event) {
        if (!(event instanceof RedisMessaging)) {
            log.error(event.getClass().getName()+" is not RedisMessaging");
            return null;
        }
        RedisMessaging e = (RedisMessaging) event;
        String key = e.key();        
        Class<? extends ExecutableBean> beanClass = beanClassMap.get(key);
        if (null == beanClass) {
            log.error("Has not corresponding MysqlBean Class for " + key);
            return null;
        }        
        String message = e.message();
        ExecutableBean bean = gson.fromJson(message, beanClass);
        if (null == bean) {
            log.error(message + " can not convert to "+beanClass.getName());
            return null;
        }

        return bean.execute(conn, beanContext);
    }

    private final static Log log = LogFactory.getLog(MessagePersistence.class);
    
    private final Gson gson;
    private final Map<String, Class<? extends ExecutableBean>> beanClassMap;

    private BeanContext beanContext = null;
}