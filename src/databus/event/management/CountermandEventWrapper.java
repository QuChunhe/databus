package databus.event.management;

import databus.core.Publisher;
import databus.event.CountermandEvent;

public class CountermandEventWrapper extends AbstractManagementEvent 
                                    implements CountermandEvent{

    public CountermandEventWrapper(String topic) {
        super(topic);
    }
    
    public CountermandEventWrapper() {
        this(null);
    }

    @Override
    public String type() {
        return Type.COUNTERMAND.toString();
    }

    @Override
    public void execute(Publisher pulisher) {
        // TODO Auto-generated method stub
        
    }
}
