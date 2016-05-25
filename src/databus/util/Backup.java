package databus.util;


import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
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
 
    public void store(String recordedId, String...recordedPairs) {
        int len = recordedPairs.length;
        if ((len%2) != 0) {
            log.error("recoredPairs must be even!");
            return;
        }
        Recorder recorder = getRecorder(recordedId);
        if (null == recorder) {
            log.error("Can't record : "+Arrays.toString(recordedPairs));
            return;
        }
        Properties properties = recorder.loadProperties();
        for(int i=0; i<len; i+=2) {
            properties.put(recordedPairs[i], recordedPairs[i+1]);
        }
        recorder.saveProperties(properties);                
    }
    
    public Properties restore(String recordedId) {
        Recorder recorder = getRecorder(recordedId);
        if (null != recorder) {
            return recorder.loadProperties();
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
            try {
                recorder = new Recorder(getFileName(recordedId));
                recorders.put(recordedId, recorder);
            } catch (IOException e) {
                log.error("Can't create Recorder for " + recordedId);
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
