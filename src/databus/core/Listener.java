package databus.core;

public interface Listener extends Initializable, Startable, Stoppable, Joinable {

    void setPublisher(Publisher publisher);

}
