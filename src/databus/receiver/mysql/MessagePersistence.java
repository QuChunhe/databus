package databus.receiver.mysql;

import java.sql.Connection;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
        classesMap = new HashMap<String, Class<?>>();
    }    

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        
        String[] keys  = split(properties.getProperty("bean.key"));
        String[] beans = split(properties.getProperty("bean.class"));
        if ((null==keys) || (null==beans)) {
            log.error("Key or class is null in Bean");
            System.exit(1);
        }
        if (keys.length != beans.length) {
            log.error("The number of topics is not equal to beans!");
            System.exit(1);
        }
        
        int len = keys.length;
        for(int i=0; i<len; i++) {            
            String key = keys[i];
            String className = beans[i];
            try {                
                Class<?> beanClass =  Class.forName(className);                
                classesMap.put(key.trim(), beanClass);
            } catch (ClassNotFoundException e) {
                log.error("Can't find "+className+" for "+keys[i], e);
            }
        }
        
        String beanContextClassName = properties.getProperty("beanContext");
        if (null != beanContextClassName) {
            try {                
                Class<?> beanContextClass =  Class.forName(beanContextClassName);                
                beanContext = (BeanContext) beanContextClass.newInstance();
                Properties prop = new Properties();
                final String PREFIX = "beanContext";
                for(Map.Entry<Object, Object> entry : properties.entrySet()) {
                    String key = entry.getKey().toString();
                    if (key.startsWith(PREFIX)) {
                        String newKey = key.substring(PREFIX.length());
                        prop.setProperty(newKey, entry.getValue().toString());
                    }
                }
                beanContext.initialize(prop);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                log.error("Can't instantiate "+beanContextClassName, e);
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
        Class<?> beanClass = classesMap.get(key);
        if (null == beanClass) {
            log.error("Has not corresponding MysqlBean Class for " + key);
            return null;
        }        
        String message = e.message();
        Object bean = gson.fromJson(message, beanClass);
        String sql = null;
        if ((null!=bean) && (bean instanceof ExecutableBean)) {
            sql = ((ExecutableBean) bean).execute(conn, beanContext);
        } else {
            log.error(message + " can not convert to "+beanClass.getName());
        }
        return sql;
    }
    
    private String[] split(String value) {
        return value==null ? null : value.split(",");
    }
    
    private static Log log = LogFactory.getLog(MessagePersistence.class);
    
    private Gson gson;
    private Map<String,Class<?>> classesMap;
    private BeanContext beanContext = null;
}