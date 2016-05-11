package databus.application;

import java.lang.management.ManagementFactory;

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

    public static void main(String[] args) {
        System.out.println(getPid());
        try {
            Thread.sleep(60*1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
