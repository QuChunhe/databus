package databus.event.mysql;

public class ColumnAttribute { 
    
    public ColumnAttribute(int type, String typeName) {
        super();
        this.type = type;
        isUnsigned = typeName.toUpperCase().indexOf("UNSIGNED") != -1;
    }
    
    public int type() {
        return type;
    }
    
    public boolean isUnsigned() {
        return isUnsigned;
    }
    
    private int type;
    private boolean isUnsigned;

}
