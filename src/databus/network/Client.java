package databus.network;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
import static databus.network.NetConstants.TASK_CAPACITY;
import static databus.network.NetConstants.CONNECTING_LISTENER_LIMIT_PER_THREAD;
import static databus.network.NetConstants.MAX_CONNECTION_PER_THREAD;

public class Client  implements Startable {

    public Client() {
        this(1);
    }
    
    public Client(int threadPoolSize) {
        taskQueue = new LinkedBlockingQueue<Task>(TASK_CAPACITY);
        thread = new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        run0();
                                    }
                               },
                           "DataBus Client");
        group = new NioEventLoopGroup(threadPoolSize);        
        channelPoolMap = new DatabusChannelPoolMap(group, 
                                                   threadPoolSize * MAX_CONNECTION_PER_THREAD);
        eventParser = new EventParser();
        connectingLimiter = new Semaphore(CONNECTING_LISTENER_LIMIT_PER_THREAD * threadPoolSize);
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
    
    public void send(Event event, Collection<SocketAddress> destinations){
        String message = eventParser.toString(event);
        for(SocketAddress address: destinations) {
            send(message, address);
        }
        event.clear();
    }
    
    public void send(Event event, SocketAddress destination) {
        String message = eventParser.toString(event);
        send(message, destination);
        event.clear();
    }

    private void run0() {
        while (doRun) {
            try {
                connectingLimiter.acquire();
                Task task = taskQueue.take();                
                String message = task.message();
                SocketAddress address = task.socketAddress();
                ChannelPool pool = channelPoolMap.get(address);
                pool.acquire()
                    .addListener(new ConnectingListener(message, pool));
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
    
    private void send(String message, SocketAddress destination) {
        add(new Task(destination, message));
    }
    
    private void add(Task task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            log.error("LinkedBlockingQueue overflow", e);
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
     
    private BlockingQueue<Task> taskQueue;
    private volatile boolean doRun = false;
    private Thread thread;
    private EventLoopGroup group;
    private DatabusChannelPoolMap channelPoolMap;    
    private EventParser eventParser;
    private Semaphore connectingLimiter;
}