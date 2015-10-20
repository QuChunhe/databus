package test;

import java.text.DateFormat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.core.Listener;
import databus.core.Publisher;
import databus.listener.MySQLListener;

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
        System.out.println(event.topic() +
                           "=" +
                           gson.toJson(event));
        
    }
    
    public static void main(String[] args) {
        Publisher publisher = new PubliserMain();
        Listener listener = new MySQLListener(publisher);
        
        listener.start();

    }
    

}
