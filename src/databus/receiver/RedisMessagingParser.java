package databus.receiver;

import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.core.Initializable;
import databus.event.redis.RedisMessaging;


public class RedisMessagingParser implements EventParser, Initializable{  

    public RedisMessagingParser() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
        classesMap = new HashMap<String, Class<Bean>>();
    }

    @Override
    public Bean parse(Event event) {        
        if (!(event instanceof RedisMessaging)) {
            return null;
        }
        RedisMessaging e = (RedisMessaging) event;
        String key = e.key();        
        Class<Bean> beanClass = classesMap.get(key);
        if (null == beanClass) {
            return null;
        }        
        String message = e.message();
        Bean bean = gson.fromJson(message, beanClass);
        return bean;
    }

    @Override
    public void initialize(Properties properties) {
        
        
    }
    
    private Gson gson;
    private Map<String,Class<Bean>> classesMap;

}
