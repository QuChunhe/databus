package databus.network;

import io.netty.channel.Channel;
import io.netty.channel.pool.AbstractChannelPoolHandler;

public class NullChannelPoolHandler extends AbstractChannelPoolHandler{    

    public NullChannelPoolHandler() {
        super();
    }

    @Override
    public void channelCreated(Channel ch) throws Exception {
        
    }

}
