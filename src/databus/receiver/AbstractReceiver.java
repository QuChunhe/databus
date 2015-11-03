package databus.receiver;

import databus.core.Receiver;
import databus.util.RemoteTopic;

public abstract class AbstractReceiver implements Receiver{
    
    public void remoteTopic(RemoteTopic remoteTopic) {
        this.remoteTopic = remoteTopic;
    }
    
    public RemoteTopic remoteTopic() {
        return remoteTopic;
    }

    private RemoteTopic remoteTopic;
}
