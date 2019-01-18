package databus.core;

public interface Listener extends Startable, Stoppable, Joinable {

    void setPublisher(Publisher publisher);

}
