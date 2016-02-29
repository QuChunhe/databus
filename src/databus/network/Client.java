package databus.network;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import databus.core.Event;

import static databus.network.NetConstants.DELIMITER_STRING;
import static databus.network.NetConstants.DEFAULT_CONNECTING_LISTENERS_PER_THREAD;
import static databus.network.NetConstants.DEFAULT_CONNECTIONS_PER_THREAD;

public class Client {

    public Client() {
        this(1);
    }
    
    public Client(int threadPoolSize) {
        this(threadPoolSize, DEFAULT_CONNECTIONS_PER_THREAD);
    }
    
    public Client(int threadPoolSize, int connectionsPerThread) {
        this(threadPoolSize, connectionsPerThread, DEFAULT_CONNECTING_LISTENERS_PER_THREAD);
    }
    
    public Client(int threadPoolSize, int connectionsPerThread, int connectingListenersPerThread) {
        group = new NioEventLoopGroup(threadPoolSize);        
        channelPoolMap = new DatabusChannelPoolMap(group, threadPoolSize * connectionsPerThread);
        eventParser = new EventParser();
        connectingLimiter = new Semaphore(connectingListenersPerThread * threadPoolSize);
    }

    public boolean isRunning() {
        return !group.isTerminated();
    }
    
    public void awaitTermination() {
        try {
            group.awaitTermination(0, null);
        } catch (InterruptedException e) {
            log.info("Client is interrupted", e);
        }
    }
    
    public void stop() {        
        group.shutdownGracefully();
        channelPoolMap.close();        
    }
    
    public void send(Event event, Collection<SocketAddress> destinations){
        String message = eventParser.toString(event);
        for(SocketAddress address: destinations) {
            send(message, address);
        }
    }
    
    public void send(Event event, SocketAddress destination) {
        String message = eventParser.toString(event);
        send(message, destination);
    }

    private void send(String message, SocketAddress destination) {        
        try {
            connectingLimiter.acquire();            
            ChannelPool pool = channelPoolMap.get(destination);
            pool.acquire()
                .addListener(new ConnectingListener(message, pool));
        } catch (InterruptedException e) {
            log.warn("Has been interrupped!", e);
            Thread.interrupted();
        } catch (Exception e) {
            log.warn("Has some errors!", e);
        } 
    }
    
    private static class SendingListener implements GenericFutureListener<ChannelFuture> {

        public SendingListener(String message, ChannelPool channelPool) {
            this.message = message;
            this.channelPool = channelPool;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if(future.isDone() && future.isSuccess()) {               
                log.info("Message has sent : "+message);                    
            } else {
                
                log.error(message+" can't send", future.cause());
            }
            channelPool.release(future.channel());
        }  
        
        private String message;
        private ChannelPool channelPool;
    }
    
    private class ConnectingListener implements GenericFutureListener<Future<Channel>> {
        
        public ConnectingListener(String message, ChannelPool channelPool) {
            this.message = message;
            this.channelPool = channelPool;
        }

        @Override
        public void operationComplete(Future<Channel> future) throws Exception {
            // This must be first avoid to throw Exception.
            connectingLimiter.release();
            
            Channel channel = future.get();
            if(future.isDone() && future.isSuccess()) {
                channel.pipeline()
                       .writeAndFlush(message + DELIMITER_STRING)
                       .addListener(new SendingListener(message, channelPool));
            } else {
                channelPool.release(channel);
                log.warn(message+" can't send because connection to " + 
                         channel.remoteAddress().toString() + " is failed", future.cause());
            }                         
        }

        private String message;
        private ChannelPool channelPool;
    }
    
    private static Log log = LogFactory.getLog(Client.class);  
     
    private EventLoopGroup group;
    private DatabusChannelPoolMap channelPoolMap;    
    private EventParser eventParser;
    private Semaphore connectingLimiter;
}