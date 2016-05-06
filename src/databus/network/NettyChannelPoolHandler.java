package databus.network;

import static databus.network.NettyConstants.CHANNEL_IDLE_DURATION_SECONDS;
import static databus.network.NettyConstants.DEFAULT_ZIP;


import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;

public class NettyChannelPoolHandler extends AbstractChannelPoolHandler{    

    public NettyChannelPoolHandler() {
        super();
    }

    @Override
    public void channelCreated(Channel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new IdleStateHandler(0, 0, CHANNEL_IDLE_DURATION_SECONDS))
         .addLast(new NettyIdleConnectionHandler())
         .addLast(ZlibCodecFactory.newZlibEncoder(DEFAULT_ZIP))
         .addLast(ZlibCodecFactory.newZlibDecoder(DEFAULT_ZIP))
         .addLast(stringEncoder)
         .addLast(stringDecoder);
    }

    private StringEncoder stringEncoder = new StringEncoder(CharsetUtil.UTF_8);
    private StringDecoder stringDecoder = new StringDecoder(CharsetUtil.UTF_8);
}
