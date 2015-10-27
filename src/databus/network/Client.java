package databus.network;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    public boolean  isRunning() {
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
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class);
            
            while (doRun) {
                try {
                    Task task = taskQueue.take();
                    TransportListener listener = new TransportListener(task);
                    bootstrap.connect(task.ipAddress(), task.port())
                             .addListener(listener);
                } catch (InterruptedException e) {
                    log.warn("Has been interrupped!", e);
                    Thread.interrupted();
                }
            }

        } finally {
            group.shutdownGracefully();
        }        
    }
    
    public void addTask(Task task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            log.error("LinkedBlockingQueue overflow", e);
        }
    }

    private static class TransportListener
                              implements GenericFutureListener<ChannelFuture> {
       
        public TransportListener(Task task) {
            this.task = task;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if(future.isDone()) {
                if (future.isSuccess()) {
                    String message = task.message();
                    future.channel().write(message);
                }else {
                    log("connection has failed ", future.cause());
                }
                
            } else {
                log("cannot connect ", future.cause());
            }
            
        }

        private void log(String msg, Throwable cause) {
            log.error(msg+task.ipAddress()+":"+task.port(), cause);
        }
        
        private Task task;
    }

    private static Log log = LogFactory.getLog(Client.class);
    
    private BlockingQueue<Task> taskQueue;
    private volatile boolean doRun = false;
    private Thread thread;

}
