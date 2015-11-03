package databus.network;

import java.text.DateFormat;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.util.InternetAddress;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

public class Client  implements Runnable, Startable {

    public Client(InternetAddress listeningAddress) {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
        taskQueue = new LinkedBlockingQueue<Task>();
        thread = new Thread(this, "DataBus Client");
        this.listeningAddress = listeningAddress;
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

    @Override
    public void run() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                     .channel(NioSocketChannel.class)
                     .option(ChannelOption.TCP_NODELAY, true);
            
            while (doRun) {
                try {
                    Task task = taskQueue.take();
                    String message = task.message();
                    ClientHandler handler = new ClientHandler(message);  
                    SendingListener listener = new SendingListener(message);
                    bootstrap.handler(handler)
                             .connect(task.ipAddress(), task.port())
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
    
    public void send(Event event, Collection<InternetAddress> destinations){
        String message = stringOf(event);
        for(InternetAddress address: destinations) {
            send(message, address);
        }
    }
    
    public void send(Event event, InternetAddress destination) {
        String message = stringOf(event);
        send(message, destination);
    }
    
    private void send(String message, InternetAddress destination) {
        add(new Task(destination, message));
    }
    
    private void add(Task task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            log.error("LinkedBlockingQueue overflow", e);
        }
    }

    private String stringOf(Event e) {
        e.address(listeningAddress);
        return e.source().toString() + ":" + e.type() + "=" + gson.toJson(e);
    }
    
    private static class SendingListener 
                              implements GenericFutureListener<ChannelFuture> {

        public SendingListener(String message) {
            this.message = message;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            String address = future.channel().remoteAddress().toString();
            if(future.isDone()) {
                if (future.isSuccess()) {
                    log.info("Message have sent to "+address+" : "+message);
                }else {
                    log.error(message+" has faied to send "+address, future.cause());
                }
                
            } else {
                log.error(message+"cannot send to "+address, future.cause());
            }            
        }  
        
        private String message;
    }

    private static Log log = LogFactory.getLog(Client.class);
    
    private BlockingQueue<Task> taskQueue;
    private volatile boolean doRun = false;
    private Gson gson;
    private Thread thread;
    private InternetAddress listeningAddress;
}
