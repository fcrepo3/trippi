package org.trippi;

import java.io.File;
import java.io.FileInputStream;

import java.util.Properties;

import org.trippi.config.TrippiConfig;
import org.trippi.config.TrippiProfile;

/**
 * Provides static access to test configuration.
 *
 * The source of the test configuration is the <code>test.properties</code>
 * file, which is exprected to be located in directory given by the
 * the <code>test.dir</code> system property.
 *
 * @author cwilper@cs.cornell.edu
 */
public abstract class TestConfig {

    private static boolean _initialized = false;

    private TestConfig() { }

    /**
     * Import system properties from ${test.dir}/test.properties
     * and set derby.system.home to ${test.dir}/derby.
     */
    private static void init() {

        if (!_initialized) {

            try {

                File testPropFile = new File(getTestDir(), "test.properties");
                System.getProperties().load(new FileInputStream(testPropFile));

                System.setProperty("derby.system.home", 
                                   new File(getTestDir(), "derby").getPath());

                _initialized = true;

            } catch (Throwable th) {
                throw new RuntimeException("Test initialization failed", th);
            }

        }

    }

    /**
     * Get a system property value, or <code>null</code> if not required
     * and undefined.
     */
    private static String getProp(String name, boolean required) {
        String value = System.getProperty(name);
        if (required && (value == null || value.equals(""))) {
            throw new RuntimeException("Property must be defined: " + name);
        } else {
            return value;
        }
    }

    /**
     * Get the test directory, as defined by the <code>test.dir</code>
     * system property.
     */
    public static File getTestDir() {
        String testDir = getProp("test.dir", true);
        return new File(testDir);
    }

    /**
     * Get the Trippi Profile we're testing with.
     *
     * The <i>profileId</i> is given by <code>test.profile</code>.
     * The profile-specific configuration is given by the
     * <code>test.<i>profileId</i>.*</code> properties.
     */
    public static TrippiProfile getTestProfile() {
        init();
        try {
            String id = getProp("test.profile", true);
            TrippiProfile profile = TrippiConfig.getProfile(
                    System.getProperties(), id, id);
            if (profile != null) {
                return profile;
            } else {
                throw new Exception("Config missing or incomplete for Trippi "
                        + "profile: " + id);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error getting Trippi Profile", e);
        }
    }

}