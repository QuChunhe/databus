package databus.event;

import java.util.List;

public interface MysqlUpdateEvent extends MysqlWriteEvent<MysqlUpdateEvent.Entity> {

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

}
