package databus.event.management;

public class Subscription extends AbstractMgtEvent {

    public Subscription() {
        super();
    }

    @Override
    public String type() {
        return Type.SUBSCRIPTION.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(128);
        builder.append("{")
               .append("\"time\": ")
               .append(time())
               .append(", ")
               .append("\"ipAddress\": \"")
               .append(ipAddress())
               .append("\", ")
               .append("\"port\": ")
               .append(port)
               .append(", ")
               .append("\"source\": \"")
               .append(source())
               .append("\", ")
               .append("\"topic\": \"")
               .append(topic())
               .append("\"}");
        return builder.toString();
    }
    
    public int port() {
        return port;
    }
    
    public void port(int port) {
        this.port = port;
    }
    
    private int port;    
}
