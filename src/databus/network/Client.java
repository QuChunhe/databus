package databus.network;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.DateFormat;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.util.InternetAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;


public class Client  implements Startable {

    public Client() {
        this(1);
    }
    
    public Client(int threadPoolSize) {
        gson = new GsonBuilder().enableComplexMapKeySerialization()
                                .serializeNulls()
                                .setDateFormat(DateFormat.LONG)
                                .addSerializationExclusionStrategy(new ExclusionStrategy() {
                                    @Override
                                    public boolean shouldSkipClass(Class<?> clazz) {
                                        return false;
                                    }

                                    @Override
                                    public boolean shouldSkipField(FieldAttributes f) {
                                        return "ipAddress".equals(f.getName());
                                    }                                    
                                })
                                .create();
        taskQueue = new LinkedBlockingDeque<Task>();
        thread = new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        run0();
                                    }
                               },
                           "DataBus Client");
        group = new NioEventLoopGroup(threadPoolSize);
        channelPoolMap = new DatabusChannelPoolMap(group, threadPoolSize);
    }
    
    @Override
    public Thread start() {
        if (false == doRun) {
            doRun = true;
            thread.start(); 
        }
        return thread;
    }

    public boolean isRunning() {
        return doRun;
    }
    
    public void stop() {
        if (true == doRun) {
            doRun = false;
            thread.interrupt();
        }
    }
    
    public void send(Event event, Collection<InternetAddress> destinations){
        ByteBuf buffer = netUtil.compress(stringOf(event));
        for(InternetAddress address: destinations) {
            ByteBuf duplicatedBuffer = buffer.duplicate();
            duplicatedBuffer.readerIndex(buffer.readerIndex());
            duplicatedBuffer.writerIndex(buffer.writerIndex());
            send(duplicatedBuffer, address);
        }
    }
    
    public void send(Event event, InternetAddress destination) {
        ByteBuf buffer = netUtil.compress(stringOf(event));
        send(buffer, destination);
    }

    private void run0() {
        while (doRun) {
            try {
                Task task = taskQueue.take();                
                ByteBuf buffer = task.buffer();
                SocketAddress address = new InetSocketAddress(task.ipAddress(), task.port());
                ChannelPool pool = channelPoolMap.get(address);
                pool.acquire()
                    .addListener(new ConnectingListener(buffer, pool));                    
            } catch (InterruptedException e) {
                log.warn("Has been interrupped!", e);
                Thread.interrupted();
            } catch (Exception e) {
                log.warn("Has some errors!", e);
            }
        }
   
        group.shutdownGracefully();
        channelPoolMap.close(); 
    }
    
    private void send(ByteBuf buffer, InternetAddress destination) {
        add(new Task(destination, buffer));
    }
    
    private void add(Task task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            log.error("LinkedBlockingQueue overflow", e);
        }
    }

    private String stringOf(Event e) {
        return e.source().toString() + ":" + e.type() + "=" + gson.toJson(e);
    }
    
    private static class SendingListener implements GenericFutureListener<ChannelFuture> {

        public SendingListener(ByteBuf buffer, ChannelPool channelPool) {
            this.buffer = buffer;
            this.channelPool = channelPool;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if(future.isDone() && future.isSuccess()) {               
//                log.info("Message has sent : "+netUtil.decompress(buffer));                    
            } else {
                log.warn(netUtil.decompress(buffer)+" can't send", future.cause());
            }
            channelPool.release(future.channel());
        }  
        
        private ByteBuf buffer;
        private ChannelPool channelPool;
    }
    
    private static class ConnectingListener implements GenericFutureListener<Future<Channel>> {
        
        public ConnectingListener(ByteBuf buffer, ChannelPool channelPool) {
            this.buffer = buffer;
            this.channelPool = channelPool;
        }

        @Override
        public void operationComplete(Future<Channel> future) throws Exception {
            Channel channel = future.get();
            if(future.isDone() && future.isSuccess()) {
                channel.writeAndFlush(buffer)
                       .addListener(new SendingListener(buffer, channelPool));
            } else {
                channelPool.release(channel);
                log.warn(netUtil.decompress(buffer)+" can't send because connection to " + 
                         channel.remoteAddress().toString() + " is failed", future.cause());
            }
        }
        
        private ByteBuf buffer;
        private ChannelPool channelPool;
    }    
    
    private static Log log = LogFactory.getLog(Client.class);
    private static NetUtil netUtil = new NetUtil();
    
    private BlockingQueue<Task> taskQueue;
    private volatile boolean doRun = false;
    private Gson gson;
    private Thread thread;
    private EventLoopGroup group;
    private DatabusChannelPoolMap channelPoolMap;
    
}