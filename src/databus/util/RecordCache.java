package databus.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RecordCache { 
    
    public RecordCache(Recorder recorder) {
        cache = new ConcurrentHashMap<String, String>(recorder.load());
        this.recorder = recorder;
    }
    
    public RecordCache cache(String key, String value) {
        cache.put(key, value);
        return this;
    }
    
    public RecordCache cache(Map<String, String> data) {
        cache.putAll(data);
        return this;
    }
    
    public String get(String key) {
        return cache.get(key);
    }
    
    public RecordCache save() {
        recorder.save(cache);
        return this;
    }
    
    public Map<String, String> copyCache(String key) {
        return new HashMap<String, String>(cache);
    }

    private ConcurrentHashMap<String, String> cache;
    private Recorder recorder;

}
