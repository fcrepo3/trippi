package org.trippi.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.trippi.TrippiException;

/**
 * Persistent configuration for working with one or more
 * Trippi-enabled stores and holding onto a set of aliases.
 */
public class TrippiConfig {

    private File m_file;

    public TrippiConfig(File configFile) {
        m_file = configFile;
    }

    //////////////////////////////////////////////////////////////////////////
    // Profiles
    //////////////////////////////////////////////////////////////////////////

    /**
     * Get a map of named <code>TrippiProfile</code>s from the config file.
     */
    public Map<String, TrippiProfile> getProfiles() throws TrippiException {
        Properties p = loadProps();
        Map<String, TrippiProfile> profileMap = new HashMap<String, TrippiProfile>();
        Enumeration<?> e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("profile.") && key.endsWith(".label")) {
                String id = key.substring(8, key.lastIndexOf("."));
                profileMap.put(id, getProfile(p, id, p.getProperty(key)));
            }
        }
        return profileMap;
    }
    
    public static Map<String, TrippiProfile> getProfiles(Properties p) {
    	Map<String, TrippiProfile> profileMap = new HashMap<String, TrippiProfile>();
        Enumeration<?> e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("profile.") && key.endsWith(".label")) {
                String id = key.substring(8, key.lastIndexOf("."));
                profileMap.put(id, getProfile(p, id, p.getProperty(key)));
            }
        }
        return profileMap;
    }
    
    public static TrippiProfile getProfile(Properties p, String id, String label) {
        String connectorClassName = null;
        String configStart = "profile." + id + ".config.";
        Map<String, String> config = new HashMap<String, String>();
        Enumeration<?> e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.equals("profile." + id + ".connectorClassName")) {
                connectorClassName = p.getProperty(key);
            } else if (key.startsWith(configStart)) {
                String realKey = key.substring(configStart.length());
                config.put(realKey, p.getProperty(key));
            }
        }
        if (label == null || connectorClassName == null) return null;
        return new TrippiProfile(id, label, connectorClassName, config);
    }

    /**
     * Set the profiles in the config file.
     */
    public void setProfiles(Map<String, TrippiProfile> profiles) throws TrippiException {
        Properties p = loadProps();
        clearProfiles(p);
        Iterator<String> ids = profiles.keySet().iterator();
        while (ids.hasNext()) {
            String id = ids.next();
            putProfile(p, profiles.get(id));
        }
        saveProps(p);
    }

    private static void clearProfiles(Properties p) {
        List<String> keysToRemove = new ArrayList<String>();
        Enumeration<?> e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("profile.")) keysToRemove.add(key);
        }
        Iterator<String> iter = keysToRemove.iterator();
        while (iter.hasNext()) {
            p.remove(iter.next());
        }
    }

    private static void putProfile(Properties p, 
                                   TrippiProfile profile) {
        String s = "profile." + profile.getId() + ".";
        p.setProperty(s + "label", profile.getLabel());
        p.setProperty(s + "connectorClassName", profile.getConnectorClassName());
        s += "config.";
        Map<String, String> map = profile.getConfiguration();
        Iterator<String> iter = map.keySet().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            String val = map.get(key);
            p.setProperty(s + key, val);
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // Aliases
    //////////////////////////////////////////////////////////////////////////

    public Map<String, String> getAliasMap() throws TrippiException {
        return getAliasMap(loadProps());
    }

    private static Map<String, String> getAliasMap(Properties p) {
        Map<String, String> map = new HashMap<String, String>();
        Enumeration<?> e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("alias.")) {
                map.put(key.replaceFirst("alias.", ""), p.getProperty(key));
            }
        }
        return map;
    }

    public void setAliasMap(Map<String, String> aliasMap) throws TrippiException {
        Properties p = loadProps();
        clearAliases(p);
        Iterator<String> iter = aliasMap.keySet().iterator();
        while (iter.hasNext()) {
            String alias = iter.next();
            String value = aliasMap.get(alias);
            p.setProperty("alias." + alias, value);
        }
        saveProps(p);
    }

    private static void clearAliases(Properties p) {
        List<String> keysToRemove = new ArrayList<String>();
        Enumeration<?> e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("alias.")) keysToRemove.add(key);
        }
        Iterator<String> iter = keysToRemove.iterator();
        while (iter.hasNext()) {
            p.remove(iter.next());
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // Common private methods
    //////////////////////////////////////////////////////////////////////////

    private Properties loadProps() throws TrippiException {
        try {
            Properties p = new Properties();
            if (m_file.exists()) {
                p.load(new FileInputStream(m_file));
            }
            return p;
        } catch (Exception e) {
            throw new TrippiException("Error loading config file", e);
        }
    }

    private void saveProps(Properties p) throws TrippiException {
        try {
            p.store(new FileOutputStream(m_file), null);
        } catch (Exception e) {
            throw new TrippiException("Error saving config file", e);
        }
    }

}
