package databus.event;

import java.util.List;

public class MySQLInsertEvent extends MySQLAbstractEvent{    
    protected List<List<String>> rows;

    public List<List<String>> getInsertedRows() {
        return rows;
    }

    @Override
    public Type type() {
        return MySQLEvent.Type.INSERT;
    }

}
