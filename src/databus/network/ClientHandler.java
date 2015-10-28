package databus.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ClientHandler  extends ChannelInboundHandlerAdapter{
    
    public ClientHandler(String message) {
        super();
        this.message = message;
    } 

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                                                             throws Exception {
       String address =  ctx.channel().remoteAddress().toString();
        log.error(message+" cannot send to "+address, cause);
    }
    
    private static Log log = LogFactory.getLog(ClientHandler.class);
    
    private String message;
}
