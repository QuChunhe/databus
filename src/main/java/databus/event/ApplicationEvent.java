package databus.event;

/**
 * Created by Qu Chunhe on 2020-05-19.
 */
public class ApplicationEvent extends AbstractEvent {

    enum Type {
        PARAMETER
    }

    @Override
    public Source source() {
        return Source.APPLICATION;
    }

    @Override
    public String type() {
        return Type.PARAMETER.toString();
    }

}
