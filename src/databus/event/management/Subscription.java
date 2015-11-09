package databus.event.management;

public class Subscription extends AbstractMgtEvent {

    public Subscription() {
        super();
        // TODO Auto-generated constructor stub
    }

    @Override
    public String type() {
        return Type.SUBSCRIPTION.toString();
    }

}
