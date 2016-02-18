package databus.network;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.Backup;


public class BackupPublisher extends InteractivePublisher{   
    
    public BackupPublisher(Client client) {        
        super(client);
        recover();
    }
    
    @Override
    public boolean subscribe(String topic, SocketAddress remoteAddress) {
        boolean isAdded = super.subscribe(topic, remoteAddress);
        if(isAdded) {
            store();
        }
        return isAdded;
    }
    
    @Override
    public boolean withdraw(String topic, SocketAddress remoteAddress) {
        boolean isRemoved = super.withdraw(topic, remoteAddress);
        if (isRemoved) {
            store();
        }
        return isRemoved;
    }

    private void recover() {
        Map<String, Set<SocketAddress>> copy = 
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
