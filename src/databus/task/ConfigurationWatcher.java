package databus.task;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import databus.core.AbstractService;
import databus.core.Runner;

/**
 * Created by Qu Chunhe on 2017-03-31.
 */
public class ConfigurationWatcher extends AbstractService {

    public ConfigurationWatcher() {
        super();
        try {
            watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            log.error("Can not create WatchService!", e);
            System.exit(1);
        }
        targetDirectoryMap = new HashMap<>();
        contextMap = new HashMap<>();
        setRunner(new WatchingRunner(), "ConfigurationServices");
    }


    public void setTargetFiles(Collection<String> targetFiles) {
        targetDirectoryMap.clear();
        for (String tf : targetFiles) {
            try {
                File file = Paths.get(tf).toFile().getCanonicalFile();
                String absoluteName = file.getCanonicalPath();
                if (!file.isFile()) {
                    log.error(tf+" is not a file!");
                    System.exit(1);
                }
                String directory = file.getParent();
                if (null == directory) {
                    log.error(tf + " has no parent!");
                    System.exit(1);
                }
                TargetDirectory td = targetDirectoryMap.get(directory);
                if (null == td) {
                    td = new TargetDirectory(directory);
                    targetDirectoryMap.put(directory, td);
                }
                td.addTargetFile(file.getName());
                FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(tf);
                contextMap.put(absoluteName, context);
            } catch (IOException e) {
                log.error("Can not open "+tf, e);
                System.exit(1);
            }
        }
    }

    public void setPollingSeconds(long pollingSeconds) {
        this.pollingSeconds = pollingSeconds;
    }

    private final static Log log = LogFactory.getLog(ConfigurationWatcher.class);

    private final Map<String, ConfigurableApplicationContext> contextMap;
    private final Map<String, TargetDirectory> targetDirectoryMap;

    private WatchService watchService;
    private long pollingSeconds = 10;

    private class WatchingRunner implements Runner {
        @Override
        public void initialize() {
            for (String tp : targetDirectoryMap.keySet()) {
                Path path = Paths.get(tp);
                try {
                    path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                } catch (IOException e) {
                    log.error(tp+" can not register WatchServer", e);
                    System.exit(1);
                }
            }
        }

        @Override
        public void runOnce() throws Exception {
            WatchKey key = watchService.poll(pollingSeconds, TimeUnit.SECONDS);
            if (null == key) {
                return;
            }
            for (WatchEvent<?> e : key.pollEvents()) {
                if (e.kind() != StandardWatchEventKinds.ENTRY_MODIFY) {
                    continue;
                }
                try {
                    String directory = key.watchable().toString();
                    TargetDirectory targetDirectory = targetDirectoryMap.get(directory);
                    if (null != targetDirectory) {
                        Set<String> fileSet = targetDirectory.getModifiedFiles();
                        for (String f : fileSet) {
                            ConfigurableApplicationContext context = contextMap.get(f);
                            context.refresh();
                        }
                    }
                } catch (Exception ex) {
                    log.error("Has some errors!", ex);
                }
            }
            key.reset();
        }

        @Override
        public void processException(Exception e) {
            log.error("Has meet Exception!", e);
        }

        @Override
        public void processFinally() {
        }

        @Override
        public void stop(Thread owner) {
            owner.interrupt();
        }

        @Override
        public void close() {
            if (null != watchService) {
                try {
                    watchService.close();
                } catch (IOException e) {
                    log.error("Can not close WatchService!");
                }
            }

            for (ConfigurableApplicationContext context : contextMap.values()) {
                context.close();
            }
        }
    }
}
