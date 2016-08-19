package databus.application;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Joinable;
import databus.core.Stoppable;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class Startup {
    
    public static String getPid() {
        String runtimeBean = ManagementFactory.getRuntimeMXBean().getName();
        if (null == runtimeBean) {
            return null;            
        }

        String[] parts = ManagementFactory.getRuntimeMXBean().getName().split("@");
        if (parts.length < 2) {
            return null;
        }
        
        return parts[0];
    }
    
    public static void savePid(String fileName) {
        String pid = getPid();
        if (null == pid) {
            log.error("Can't get pid");
            return;
        }
        
        try (BufferedWriter writer = Files.newBufferedWriter(
                                             Paths.get(fileName), 
                                             StandardCharsets.UTF_8,
                                             StandardOpenOption.CREATE,
                                             StandardOpenOption.TRUNCATE_EXISTING,
                                             StandardOpenOption.WRITE);) {           
            writer.write(pid);
            writer.flush();
        } catch (IOException e) {
            log.error("Can't write "+fileName, e);
        }
    }
    
    public static void addShutdownHook(Stoppable hook) {
        hooks.add(hook);
    }

    public static void waitUntilSIGTERM() {
        if (hooks.size() == 0) {
            log.error("Hasn't hook to wait!");
            return;
        }
        while (isRunning) {
            try {
                for(Stoppable s : hooks) {
                    if(s instanceof Joinable) {
                        ((Joinable) s).join();
                        log.info(s.getClass().getName() +" has finished!");
                    } else {
                        log.info(s.getClass().getName()+" isn't Joinable");
                        long TEN_SECONDS = 10000;
                        Thread.sleep(TEN_SECONDS);
                    }
                }
            } catch (InterruptedException e) {
                log.warn(Thread.currentThread().getName()+" is interrupted!");
            }
        }
    }        
    
    static {
        mainThread = Thread.currentThread();
        Signal.handle(new Signal("TERM"), new SignalHandler() {
            @Override
            public void handle(Signal arg0) {
                log.info("Receiving SIGTERM");
                for(Stoppable s : hooks) {
                    s.stop();
                    log.info("Has stopped "+s.getClass().getName());
                }
                log.info("All hooks has been stopped!");
                isRunning = false;
                mainThread.interrupt();
            }            
        });
    }    
    
    
    private static Log log = LogFactory.getLog(Startup.class);
    
    private static List<Stoppable> hooks = new CopyOnWriteArrayList<Stoppable>();
    private static volatile boolean isRunning = true;
    private static Thread mainThread;
}
