package databus.network;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Startable;
import databus.util.InternetAddress;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

public class Client  implements Runnable, Startable {

    public Client() {
        taskQueue = new LinkedBlockingQueue<Task>();
        thread = new Thread(this, "DataBus Client");
    }
    
    @Override
    public void start() {
        if (false == doRun) {
            doRun = true;
            thread.start(); 
        }
    }

    @Override
    public boolean isRunning() {
        return doRun;
    }
    
    @Override
    public void stop() {
        if (true == doRun) {
            doRun = false;
            thread.interrupt();
        }
    }

    @Override
    public void run() {
        log.info("begin run");
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class);
            
            while (doRun) {
                try {
                    Task task = taskQueue.take();
                    ConnectionListener listener = new ConnectionListener(task);
                    bootstrap.connect(task.ipAddress(), task.port())
                             .addListener(listener);
                } catch (InterruptedException e) {
                    log.warn("Has been interrupped!", e);
                    Thread.interrupted();
                }
                log.info("run once");
            }

        } finally {
            group.shutdownGracefully();
        }
    }
    
    public void send(String message, Collection<InternetAddress> destinations){
        for(InternetAddress address: destinations) {
            send(message, address);
        }
    }
    
    public void send(String message, InternetAddress destination) {
        add(new Task(destination, message));
    }
    
   private void add(Task task) {
       log.info(task.toString());
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            log.error("LinkedBlockingQueue overflow", e);
        }
    }

    private static class ConnectionListener
                              implements GenericFutureListener<ChannelFuture> {
       
        public ConnectionListener(Task task) {
            this.task = task;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if(future.isDone()) {
                if (future.isSuccess()) {
                    String message = task.message();
                    future.channel()
                          .write(message).addListener(new SendingListener());
                    log.info("begin to send "+message);
                }else {
                    logError("connection has failed ", future.cause());
                }
                
            } else {
                logError("cannot connect ", future.cause());
            }
            
        }

        private void logError(String msg, Throwable cause) {
            log.error(msg+task.ipAddress()+":"+task.port(), cause);
        }
        
        private Task task;
    }
    
    private static class SendingListener 
                              implements GenericFutureListener<ChannelFuture> {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            String address = future.channel().remoteAddress().toString();
            if(future.isDone()) {
                if (future.isSuccess()) {
                    log.info("have sent to "+address);
                }else {
                    log.error(address+" sending has failed", future.cause());
                }
                
            } else {
                log.error("cannot connect to "+address, future.cause());
            }            
        }        
    }

    private static Log log = LogFactory.getLog(Client.class);
    
    private BlockingQueue<Task> taskQueue;
    private volatile boolean doRun = false;
    private Thread thread;

}
