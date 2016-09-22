package databus.core;

public interface Listener extends Initializable, Startable, Stoppable, Joinable {

    public void setFilter(EventFilter filter);
}
