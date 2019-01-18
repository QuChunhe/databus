package databus.network;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-05-31.
 */
public class ConfiguredKeyMapper implements KeyMapper {
    public ConfiguredKeyMapper() {
    }

    @Override
    public String toKey(Event event) {
        return Long.toUnsignedString(System.currentTimeMillis());
    }

    @Override
    public Class<? extends Event> toEventClass(String key) {
        return eventClassMap.get(key);
    }

    public void setEventClassMap(Map<String, String> eventClassMap) {
        this.eventClassMap = new HashMap<>();
        for(Map.Entry<String, String> entry : eventClassMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Class<? extends Event> clazz = null;
            try {
                clazz = (Class<? extends Event>) getClass().getClassLoader().loadClass(value);
            } catch (ClassNotFoundException e) {
                log.error("Can not find "+value);
                System.exit(1);
            }
            if (null == clazz) {
                log.error("Can not load "+value);
                System.exit(1);
            }
            this.eventClassMap.put(key, clazz);
        }
    }

    private final static Log log = LogFactory.getLog(ConfiguredKeyMapper.class);

    private Map<String, Class<? extends Event>> eventClassMap;
}
