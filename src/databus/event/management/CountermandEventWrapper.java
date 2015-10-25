package databus.event.management;

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
}
