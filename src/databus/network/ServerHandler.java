package databus.network;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


public class ServerHandler extends ChannelInboundHandlerAdapter {
    public ServerHandler(Publisher publisher, Subscriber subscriber) {
        super();
        this.publisher = publisher;
        this.subscriber = subscriber;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress(); 
        if (data instanceof String) {
            receive0((String) data, address);  
        } else {
            log.error(data.getClass().getName() + " isn't String from " + address.toString()+
                      " : " + data.toString());
        }             
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {     
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, 
                                Throwable cause) throws Exception {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        String localAddress = ctx.channel().localAddress().toString();
        log.error("Connection ("+localAddress+", "+remoteAddress+") has error", cause);
        ctx.close();
    }
    
    private void receive0(String message, InetSocketAddress address) {
        if ((null==message) || (message.length()==0)) {
            return;
        }
        Event event = parser.parse(message);
        if (null == event) {
            log.error("Message from " + address.toString() +
                      " cannot be parsed as Event : " + message);
            return;
        }
        
        event.ipAddress(address.getAddress());
        try {
            if (null != subscriber) {
                subscriber.receive(event);
            }
        } catch (Exception e) {
            log.error(subscriber.getClass().getName()+" can't receive event :" 
                      + event.toString(), e);           
        }
        try {
            if (null != publisher) {
                publisher.receive(event);
            }
        } catch (Exception e) {
            log.error(publisher.getClass().getName()+" can't receive event :" 
                      + event.toString(), e);            
        } 
    }
        
    private static Log log = LogFactory.getLog(ServerHandler.class);
    private static MessageParser parser = new MessageParser();
    
    private Publisher publisher;
    private Subscriber subscriber;
}
