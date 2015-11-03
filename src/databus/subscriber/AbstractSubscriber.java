package databus.subscriber;

import databus.core.Receiver;
import databus.util.RemoteTopic;

public abstract class AbstractSubscriber implements Receiver{
    
    public void remoteTopic(RemoteTopic remoteTopic) {
        this.remoteTopic = remoteTopic;
    }
    
    public RemoteTopic remoteTopic() {
        return remoteTopic;
    }

    private RemoteTopic remoteTopic;
}
