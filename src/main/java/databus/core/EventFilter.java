package databus.core;

public interface EventFilter {

    Event process(Event event);

}