package databus.util;


import java.io.IOException;
import java.util.Arrays;
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
 
    public void store(String recordedId, String...dataPairs) {
        Recorder recorder = getRecorder(recordedId);
        if (null == recorder) {
            log.error("Can't save : "+Arrays.toString(dataPairs)+" for "+recordedId);
            return;
        }
        recorder.save(dataPairs);                
    }
    
    public void store(String recordedId, Map<String, String> data) {
        Recorder recorder = getRecorder(recordedId);
        if (null == recorder) {
            log.error("Can't save : "+data.toString()+" for "+recordedId);
            return;
        }
        recorder.save(data);                
    }
    
    public String restore(String recordedId, String key) {
        Recorder recorder = getRecorder(recordedId);
        if (null != recorder) {
            return recorder.getDatum(key);
        }
        return null;
    }
    
    public Map<String, String> restore(String recordedId) {
        Recorder recorder = getRecorder(recordedId);
        if (null != recorder) {
            return recorder.getData();
        }
        return null;
    }    
    
    private String getFileName(String recordedId) {
        recordedId = recordedId.replace('.', '_');
        recordedId = recordedId.replace('/', '-');
        return BACKUP_DIR_NAME + recordedId+"_backup.data";
    }
    
    private Recorder getRecorder(String recordedId) {
        Recorder recorder = recorders.get(recordedId);
        if (null == recorder) {
            synchronized(this) {
                if (null == recorder) {
                    try {
                        recorder = new Recorder(getFileName(recordedId));
                        recorders.put(recordedId, recorder);
                    } catch (IOException e) {
                        log.error("Can't create Recorder for " + recordedId);
                    }
                }
            }            
        }
        return recorder;
    }
    
    private static final String BACKUP_DIR_NAME = "data/";
    
    private static Log log = LogFactory.getLog(Backup.class);    
    private static  Backup instance = null;    
    
    private Backup() {
        recorders = new ConcurrentHashMap<String, Recorder>();
    }
    
    private Map<String, Recorder> recorders;
}
