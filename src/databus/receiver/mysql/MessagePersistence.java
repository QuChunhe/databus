package databus.receiver.mysql;

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
        Properties beansProperties = removePrefix(properties, "beans.");
        for(String key : beansProperties.stringPropertyNames()) {
            try {
                String className = beansProperties.getProperty(key);
                Class<?> beanClass =  Class.forName(className);                
                classesMap.put(key, beanClass);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }



    @Override
    public void receive(Event event) {
        if (!(event instanceof RedisMessaging)) {
            log.error(event.getClass().getName()+" is't RedisMessaging");
            return;
        }
        RedisMessaging e = (RedisMessaging) event;
        String key = e.key();        
        Class<?> beanClass = classesMap.get(key);
        if (null == beanClass) {
            log.error(key + " has't corresponding Class");
            return;
        }        
        String message = e.message();
        Object bean = gson.fromJson(message, beanClass);
        if ((null!=bean) && (bean instanceof MysqlBean)) {
            save((MysqlBean) bean);
        } else {
            log.error(message + " can't convert to "+beanClass.getName());
        }
        
    }
    
    private void save(MysqlBean bean) {
        bean.execute(getConnection());        
    }
    
    private static Log log = LogFactory.getLog(MessagePersistence.class);
    
    private Gson gson;
    private Map<String,Class<?>> classesMap;
}
