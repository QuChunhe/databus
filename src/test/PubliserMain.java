package test;

import java.text.DateFormat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.core.Listener;
import databus.core.Publisher;
import databus.listener.MySQLListener;
import databus.network.MessageParser;
import databus.util.InternetAddress;

public class PubliserMain implements Publisher{
    Gson gson;    

    public PubliserMain() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
    }
    
    
    @Override
    public void publish(Event event) {
        System.out.println(event.type() +
                           "=" +
                           gson.toJson(event));
        
    }    
    
    
    @Override
    public void publish(InternetAddress remoteAddress, Event event) {
        // TODO Auto-generated method stub
        
    }


    public static void main(String[] args) {
        MessageParser parser = new MessageParser();
        System.exit(1);
        Publisher publisher = new PubliserMain();
        Listener listener = new MySQLListener(publisher);
        
        listener.start();
        System.out.println("!!!!!!!!!!!!!!!!!!");

    }
    

}
