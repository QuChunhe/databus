package databus.event.management;


public class Withdrawal extends AbstractMgtEvent {

    public Withdrawal() {
        super();
    }

    @Override
    public String type() {
        return Type.WITHDRAWAL.toString();
    }

    @Override
    public String toString() {
        return "Withdrawal from "+topic();
    }
}
