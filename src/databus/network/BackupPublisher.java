package databus.network;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.Backup;
import databus.util.InternetAddress;

public class BackupPublisher extends Publisher{   
    
    public BackupPublisher(Client client) {        
        super(client);
        recover();
    }
    
    @Override
    public boolean subscribe(String topic, InternetAddress remoteAddress) {
        boolean isAdded = super.subscribe(topic, remoteAddress);
        if(isAdded) {
            store();
        }
        return isAdded;
    }
    
    @Override
    public boolean withdraw(String topic, InternetAddress remoteAddress) {
        boolean isRemoved = super.withdraw(topic, remoteAddress);
        if (isRemoved) {
            store();
        }
        return isRemoved;
    }

    private void recover() {
        Map<String, Set<InternetAddress>> copy = 
                        Backup.instance().restore(BACKUP_NAME, subscribersMap);
        if (null != copy) {
            subscribersMap = copy;
            log.info(subscribersMap.toString()+ " has recovered");
        }       
    }
    
    private void store() {
        Backup.instance().store(BACKUP_NAME, subscribersMap);
        log.info(subscribersMap.toString()+" has stored");
    }
    
    private final static String BACKUP_NAME = "BackupPublisher";

    private Log log = LogFactory.getLog(BackupPublisher.class);
}
