package org.trippi.config;

import java.util.Map;

import org.trippi.TrippiException;

public class ConfigUtils {
	/**
     * Get a trimmed non-empty string from the map, or throw an exception.
     */
	public static String getRequired(Map<String, Object> map, String key) throws TrippiException {
        String value = (String) map.get(key);
        if (value == null || value.length() == 0) {
            throw new TrippiException("Missing required configuration value: " + key);
        } else {
            return value.trim();
        }
    }

    /**
     * Get an integer from the map, or throw an exception.
     */
    public static int getRequiredInt(Map<String, Object> map, String key) throws TrippiException {
        try {
            return Integer.parseInt(getRequired(map, key));
        } catch (NumberFormatException e) {
            throw new TrippiException("Configuration value must be an integer: " + key);
        }
    }
    
    /**
     * Get a non-negative integer from the map, or throw an exception.
     */
    public static int getRequiredNNInt(Map<String, Object> map, String key) throws TrippiException {
        try {
            int i = Integer.parseInt(getRequired(map, key));
            if (i < 0) {
            	throw new TrippiException("Value must not be negative.");
            } else {
            	return i;
            }
        } catch (NumberFormatException e) {
            throw new TrippiException("Configuration value must be an integer: " + key);
        }
    }
    
    /**
     * Get an integer greater than 0 from the map, or throw an exception.
     */
    public static int getRequiredPosInt(Map<String, Object> map, String key) throws TrippiException {
        try {
            int i = Integer.parseInt(getRequired(map, key));
            if (i < 1) {
            	throw new TrippiException("Value must be an integer greater than 0.");
            } else {
            	return i;
            }
        } catch (NumberFormatException e) {
            throw new TrippiException("Configuration value must be an integer: " + key);
        }
    }
    
    /**
     * 
     * @param map
     * @param key
     * @return the boolean value of the key from the map.
     * @throws TrippiException
     */
    public static boolean getRequiredBoolean(Map<String, Object> map, String key) throws TrippiException {
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
