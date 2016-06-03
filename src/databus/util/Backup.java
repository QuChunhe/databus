package databus.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Backup {
    
    public static Backup instance() {
        if (null == instance) {
            synchronized(Backup.class) {
                if (null == instance) {
                    instance = new Backup();
                }
            }
        }
        return instance;
    }    
 
    public void store(String id, final String...dataPairs) {        
        int len = dataPairs.length;
        if ((len%2) != 0) {
            log.error("recoredPairs must be even!");
            return;
        }
        RecordCache recorderCache = getRecordCache(id);
        for(int i=0; i<len; i+=2) {
            recorderCache.cache(dataPairs[i], dataPairs[i+1]);
        } 
        recorderCache.save();                
    }
    
    public void store(String id, Map<String, String> data) {
        getRecordCache(id).cache(data).save();                
    }
    
    public Map<String, String> restore(String id) {
        return getRecordCache(id).copyCache(id);
    } 
    
    public RecordCache getRecordCache(String id) {
        RecordCache recorderCache = recordCaches.get(id);
        if (null == recorderCache) {
            synchronized(this) {
                if (null == recorderCache) { 
                    recorderCache = new RecordCache(new Recorder(getFileName(id)));
                    recordCaches.put(id, recorderCache);                
                }
            }            
        }
        return recorderCache;
    }
    
    public void save(Collection<String> ids) {
        for(String id : ids) {
            getRecordCache(id).save();
        }
    }
    
    private String getFileName(String id) {
        id = id.replace('.', '_');
        id = id.replace('/', '-');
        return BACKUP_DIR_NAME + id+"_backup.data";
    }
    
    private static final String BACKUP_DIR_NAME = "data/";
    
    private static Log log = LogFactory.getLog(Backup.class);    
    private static Backup instance = null;    
    
    private Backup() {
        recordCaches = new ConcurrentHashMap<String, RecordCache>();
    }
    
    private Map<String, RecordCache> recordCaches;
}
