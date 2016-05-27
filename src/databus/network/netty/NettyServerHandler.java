package databus.network.netty;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.network.JsonEventParser;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


public class NettyServerHandler extends ChannelInboundHandlerAdapter {
    public NettyServerHandler(NettySubscriber subscriber) {
        super();
        this.subscriber = subscriber;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress(); 
        if (data instanceof String) {
            receive0((String) data, address);
            log.info("Have receive : "+data.toString());
        } else {
            log.error(data.getClass().getName() + " isn't String from " + address.toString()+
                      " : " + data.toString());
        }             
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        String localAddress = ctx.channel().localAddress().toString();
        log.error("Connection ("+localAddress+", "+remoteAddress+") has error", cause);
        ctx.close();
    }
    
    private void receive0(String frame, InetSocketAddress address) {
        if ((null==frame) || (frame.length()==0)) {
            return;
        }
        Event event = eventParser.toEvent(frame);
        if (null == event) {
            log.error("Message from " + address.toString() +
                      " cannot be parsed as Event : " + frame);
            return;
        }
        
        event.ipAddress(address.getAddress());
        try {         
            subscriber.receive(event);
        } catch (Exception e) {
            log.error(subscriber.getClass().getName()+" can't receive event :" 
                      + event.toString(), e);           
        }

    }
        
    private static Log log = LogFactory.getLog(NettyServerHandler.class);
    private static JsonEventParser eventParser = new JsonEventParser();
    
    private NettySubscriber subscriber;
}
