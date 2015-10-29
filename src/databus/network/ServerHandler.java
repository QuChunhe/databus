package databus.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Subscriber;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

/**
 * 
 * SeverHandler is thread-safe.
 *
 */
@Sharable
public class ServerHandler  extends ChannelInboundHandlerAdapter{

    public ServerHandler(Subscriber subscriber) {
        this.subscriber = subscriber;
        parser = new MessageParser();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, 
                                                Object data) throws Exception {
        ByteBuf in;
        if (data instanceof ByteBuf) {
             in = (ByteBuf) data;
        } else {
            log.error(data.getClass().getName()+" cannot cast to ByteBuf");
            return;
        }
        
        String message = in.toString(CharsetUtil.UTF_8);
        log.info("Have received message : "+message);
        Event event = parser.parse(message);
        if (null == event) {            
            log.error("Message from "+ctx.channel().remoteAddress().toString()+
                      " cannot be parsed as Event : "+message);
            return;
        }
        subscriber.receive(event);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
                                    throws Exception {
       ctx.flush();
    } 

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, 
                                Throwable cause) throws Exception {
        String address = ctx.channel().remoteAddress().toString();
        log.error("Cannot read from "+address, cause);
    }
    
    private static Log log = LogFactory.getLog(ServerHandler.class);
    private MessageParser parser;
    private Subscriber subscriber;

}
