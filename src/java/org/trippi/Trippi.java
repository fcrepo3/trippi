package org.trippi;

import java.util.Properties;

public abstract class Trippi {

    /**
     * Current version of Trippi.
     */
    public static String VERSION;
    
    static {
        try {
            Properties props = new Properties();
            props.load( ClassLoader.getSystemClassLoader()
                                   .getResourceAsStream(
                                    "org/trippi/Trippi.properties"));
            VERSION = props.getProperty("trippi.version", "unknown");
        } catch (Exception e) {
            System.err.println("Unable to load properties from jar!");
            e.printStackTrace();
            VERSION = "unknown";
        }
    }
    
    public static void main(String[] args) {
        printVersion();
    }
    
    public static void printVersion() {
        System.out.println("Trippi version " + VERSION);
    }
    
}