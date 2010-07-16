package org.trippi.config;

import java.util.Map;

import org.trippi.TrippiException;

public class ConfigUtils {
	/**
     * Get a trimmed non-empty string from the map, or throw an exception.
     */
	public static String getRequired(Map<String, String> map, String key) throws TrippiException {
        String value = map.get(key);
        if (value == null || value.length() == 0) {
            throw new TrippiException("Missing required configuration value: " + key);
        } else {
            return value.trim();
        }
    }

    /**
     * Get an integer from the map, or throw an exception.
     */
    public static int getRequiredInt(Map<String, String> map, String key) throws TrippiException {
        try {
            return Integer.parseInt(getRequired(map, key));
        } catch (NumberFormatException e) {
            throw new TrippiException("Configuration value must be an integer: " + key);
        }
    }
    
    public static int getRequiredIntGreaterThan(Map<String, String> map, String key, int floor)
        throws TrippiException {
    	int i = getRequiredInt(map, key);
    	if (i > floor){
    		return i;
    	}
    	else {
    		throw new TrippiException(key + " configuration value must be greater than " + floor);
    	}
    }
    
    
    
    /**
     * Get a non-negative integer from the map, or throw an exception.
     */
    public static int getRequiredNNInt(Map<String, String> map, String key) throws TrippiException {
        return getRequiredIntGreaterThan(map, key, -1);
    }
    
    /**
     * Get an integer greater than 0 from the map, or throw an exception.
     */
    public static int getRequiredPosInt(Map<String, String> map, String key) throws TrippiException {
        return getRequiredIntGreaterThan(map, key, 0);
    }
    
    /**
     * 
     * @param map
     * @param key
     * @return the boolean value of the key from the map.
     * @throws TrippiException
     */
    public static boolean getRequiredBoolean(Map<String, String> map, String key) throws TrippiException {
        String val = getRequired(map, key);
        if (val.equalsIgnoreCase("true")) {
            return true;
        } else if (val.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new TrippiException("Expected boolean for " + key + ", but got " + val);
        }
    }
}
