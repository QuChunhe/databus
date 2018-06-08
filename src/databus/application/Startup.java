package databus.application;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Service;

public class Startup {

    static {
        final String LOG_CONFIG_FILE =  System.getProperty("user.dir") + "/conf/log4j2.xml";
        if (new File(LOG_CONFIG_FILE).exists()) {
            System.setProperty("log4j.configurationFile", LOG_CONFIG_FILE);
        }
    }

    public void addServices(Service service) {
        services.add(service);
    }

    public void setServices(Collection<Service> services) {
        this.services = new CopyOnWriteArrayList<>(services);
    }

    public void run(String pidFileName) {
        log.info("********************Databus Will Begin!**************************************");
        savePid(pidFileName);

        for (Service s : services) {
            s.start();
        }

        registerSigTermMHander();
        waitUntilSigTerm();
        log.info("********************Databus Will End!****************************************");
    }

    private String getPid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        if (null == runtimeBean) {
            return null;            
        }
        String[] parts = runtimeBean.getName().split("@");
        if (parts.length < 2) {
            return null;
        }
        return parts[0];
    }
    
    private void savePid(String fileName) {
        String pid = getPid();
        if (null == pid) {
            log.error("Can not get pid!");
            return;
        }
        
        try (BufferedWriter writer = Files.newBufferedWriter(
                                             Paths.get(fileName), 
                                             StandardCharsets.UTF_8,
                                             StandardOpenOption.CREATE,
                                             StandardOpenOption.TRUNCATE_EXISTING,
                                             StandardOpenOption.WRITE)) {
            writer.write(pid);
            writer.flush();
        } catch (IOException e) {
            log.error("Can not write "+fileName, e);
        }
    }

    private void waitUntilSigTerm() {
        if (services.size() == 0) {
            log.error("Has not hook to wait!");
            return;
        }
        while (isRunning) {
            try {
                for(Service s : services) {
                    s.join();
                    log.error(s.getClass().getName()+" has stop!");
                }
            } catch (InterruptedException e) {
                log.warn(Thread.currentThread().getName()+" is interrupted!");
            }
        }
    }        
    
    private void registerSigTermMHander() {
        Thread mainThread = Thread.currentThread();
        Signal.handle(new Signal("TERM"), new SignalHandler() {
            @Override
            public void handle(Signal arg0) {
                log.info("Receiving SIGTERM");
                isRunning = false;
                for(Service s: services) {
                    s.stop();
                    log.info("Has stopped "+s.getClass().getName());
                }
                log.info("All endpoints has been stopped!");
                mainThread.interrupt();
            }            
        });
    } 
    
    private final static Log log = LogFactory.getLog(Startup.class);
    
    private List<Service> services = new CopyOnWriteArrayList<>();
    private volatile boolean isRunning = true;
}
