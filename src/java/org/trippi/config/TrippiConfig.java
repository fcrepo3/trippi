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
    public Map getProfiles() throws TrippiException {
        Properties p = loadProps();
        Map profileMap = new HashMap();
        List idList = new ArrayList();
        Enumeration e = p.propertyNames();
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
        Map config = new HashMap();
        Enumeration e = p.propertyNames();
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
    public void setProfiles(Map profiles) throws TrippiException {
        Properties p = loadProps();
        clearProfiles(p);
        Iterator ids = profiles.keySet().iterator();
        while (ids.hasNext()) {
            String id = (String) ids.next();
            putProfile(p, (TrippiProfile) profiles.get(id));
        }
        saveProps(p);
    }

    private static void clearProfiles(Properties p) {
        List keysToRemove = new ArrayList();
        Enumeration e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("profile.")) keysToRemove.add(key);
        }
        Iterator iter = keysToRemove.iterator();
        while (iter.hasNext()) {
            p.remove((String) iter.next());
        }
    }

    private static void putProfile(Properties p, 
                                   TrippiProfile profile) {
        String s = "profile." + profile.getId() + ".";
        p.setProperty(s + "label", profile.getLabel());
        p.setProperty(s + "connectorClassName", profile.getConnectorClassName());
        s += "config.";
        Map map = profile.getConfiguration();
        Iterator iter = map.keySet().iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            String val = (String) map.get(key);
            p.setProperty(s + key, val);
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // Aliases
    //////////////////////////////////////////////////////////////////////////

    public Map getAliasMap() throws TrippiException {
        return getAliasMap(loadProps());
    }

    private static Map getAliasMap(Properties p) {
        Map map = new HashMap();
        Enumeration e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("alias.")) {
                map.put(key.replaceFirst("alias.", ""), p.getProperty(key));
            }
        }
        return map;
    }

    public void setAliasMap(Map aliasMap) throws TrippiException {
        Properties p = loadProps();
        clearAliases(p);
        Iterator iter = aliasMap.keySet().iterator();
        while (iter.hasNext()) {
            String alias = (String) iter.next();
            String value = (String) aliasMap.get(alias);
            p.setProperty("alias." + alias, value);
        }
        saveProps(p);
    }

    private static void clearAliases(Properties p) {
        List keysToRemove = new ArrayList();
        Enumeration e = p.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (key.startsWith("alias.")) keysToRemove.add(key);
        }
        Iterator iter = keysToRemove.iterator();
        while (iter.hasNext()) {
            p.remove((String) iter.next());
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
