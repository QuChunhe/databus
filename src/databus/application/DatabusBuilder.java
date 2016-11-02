package databus.application;

import databus.core.*;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;

public class DatabusBuilder {

    public DatabusBuilder() {
        this("conf/databus.xml");
    }

    public DatabusBuilder(String configFile) {
        try {
            FileBasedConfigurationBuilder<XMLConfiguration> builder =
                    new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                            .configure(new Parameters().xml().setFileName(configFile));
            xmlConfig = builder.getConfiguration();
        } catch (ConfigurationException e) {
            log.error("Can not load " + configFile, e);
            System.exit(1);
        }
        loadGlobalParameters();
    }

    public boolean hasPublisher() {
        return xmlConfig.containsKey("publisher.class");
    }

    public Publisher createPublisher() {
        Initializable object = loadInitialiableObject(xmlConfig.configurationAt("publisher"));
        if ((null!=object) && (object instanceof Publisher)) {
        } else {
            log.error("Can not instantiate Publisher!");
            System.exit(1);
        }
        Publisher publisher = (Publisher) object;
        loadListeners(publisher);
        return publisher;
    }

    public boolean hasSubscriber() {
        return xmlConfig.containsKey("subscriber.class");
    }

    public Subscriber createSubscriber() {
        Initializable object = loadInitialiableObject(xmlConfig.configurationAt("subscriber"));
        if ((null!=object) && (object instanceof Subscriber)) {
        } else {
            log.error("Can not instantiate Subscriber!");
            System.exit(1);
        }
        Subscriber subscriber = (Subscriber) object;
        loadReceivers(subscriber);
        return subscriber;
    }

    private void loadListeners(Publisher publisher) {
        for (HierarchicalConfiguration<ImmutableNode> c :
                xmlConfig.configurationsAt("publisher.listener")) {
            Initializable object = loadInitialiableObject(c);
            if ((null!=object) && (object instanceof Listener)) {
                Listener listener = (Listener) object;
                publisher.addListener(listener);
                listener.start();
            } else {
                log.error("Can't instantiate Listener : " + c.toString());
            }
        }
    }

    private void loadReceivers(Subscriber subscriber) {
        for (HierarchicalConfiguration<ImmutableNode> sc :
                xmlConfig.configurationsAt("subscriber.receiver")) {
            Initializable object = loadInitialiableObject(sc);
            if ((null!=object) && (object instanceof Receiver)) {
                Receiver receiver = (Receiver) object;
                String[] remoteTopics = sc.getStringArray("remoteTopic");
                for (String topic : remoteTopics) {
                    subscriber.register(topic, receiver);
                }
            } else {
                log.error("Can't instantiate Receiver: " + sc.toString());
                System.exit(1);
            }
        }
    }

    private Initializable loadInitialiableObject(HierarchicalConfiguration<ImmutableNode> config) {
        if (null == config) {
            log.error("Configuration is null!");
            return null;
        }
        String className = config.getString("class");
        if (null == className) {
            log.error("Does not configure class element!");
            return null;
        }
        className = className.trim();
        if (className.length() == 0) {
            log.error("class element is empty!");
            return null;
        }
        Initializable instance = null;
        try {
            instance = (Initializable) Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            log.error("Can not instantiate " + className, e);
        } catch (IllegalAccessException e) {
            log.error("Can not access Class " + className, e);
        } catch (ClassNotFoundException e) {
            log.error("Can not find " + className, e);
        }

        if (null != instance) {
            Properties properties = ConfigurationConverter.getProperties(config);
            instance.initialize(properties);
            // Must after invoke initialize method
            for (HierarchicalConfiguration<ImmutableNode> c : config.configurationsAt("setter")) {
                String id = c.getString("parameter");
                String method = c.getString("method");
                if ((null==id) || (null==method)) {
                    log.error("id or method is null : "+ConfigurationConverter.getProperties(c));
                    System.exit(1);
                }
                setParameter(instance, method, id);
            }
        }
        return instance;
    }

    private void loadGlobalParameters() {
        globalParameters = new HashMap<>();
        for (HierarchicalConfiguration<ImmutableNode> c :
                xmlConfig.configurationsAt("global")) {
            String id = c.getString("id");
            if (null == id) {
                log.error("id is null : "+ConfigurationConverter.getProperties(c));
                System.exit(1);
            }
            Initializable object = loadInitialiableObject(c);
            if (null == object) {
                log.error("Can not instantiate an Initializable object : "+
                          ConfigurationConverter.getProperties(c));
                System.exit(1);
            }
            globalParameters.put(id, object);
        }
    }

    private void setParameter(Object object, String methodName, String id) {
        try {
            Initializable parameter = globalParameters.get(id);
            if (null == parameter) {
                log.error("Can not global parameter for "+id);
                System.exit(1);
            }
            Class<? extends Initializable> clazz = parameter.getClass();
            Method method = object.getClass().getMethod(methodName, clazz);
            method.invoke(object, parameter);
        } catch (NoSuchMethodException |InvocationTargetException | IllegalAccessException  e) {
            log.error("Can not invoke setter!", e);
            System.exit(1);
        }
    }

    private static Log log = LogFactory.getLog(DatabusBuilder.class);

    private XMLConfiguration xmlConfig;
    private Map<String, Initializable> globalParameters;
}
