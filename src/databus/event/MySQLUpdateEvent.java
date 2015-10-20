package databus.event;

import java.util.List;

public class MySQLUpdateEvent extends MySQLWriteEvent<MySQLUpdateEvent.Entity> {
    
    public MySQLUpdateEvent(long serverId, String databaseName,
                            String tableName) {
        super(serverId, databaseName, tableName);
    }

    public static class Entity {        
        
        public Entity(List<String> before, List<String> after) {
            this.before = before;
            this.after = after;
        }
        
        public List<String> before() {
            return before;
        }
        
        public List<String> after() {
            return after;
        }
        
        private List<String> before;
        private List<String> after;
    }

    @Override
    public Type type() {
        return MySQLEvent.Type.UPDATE;
    }

}
