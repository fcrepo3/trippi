package org.trippi;

import java.util.Properties;

public abstract class Trippi {

    /**
     * Current version of Trippi.
     */
    public static String VERSION = "unknown";
    
    /**
     * Date it was built.
     */
    public static String BUILD_DATE = "unknown";
    
    static {
        try {
            Properties props = new Properties();
            props.load( ClassLoader.getSystemClassLoader()
                                   .getResourceAsStream(
                                    "org/trippi/Trippi.properties"));
            VERSION = props.getProperty("trippi.version", "unknown");
            BUILD_DATE = props.getProperty("buildDate", "unknown");
        } catch (Exception e) {
            System.err.println("Unable to load Trippi.properties from jar!");
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        printVersion();
    }
    
    public static void printVersion() {
        System.out.println("Trippi version " + VERSION + " ["
                + BUILD_DATE + "]");
    }
    
}