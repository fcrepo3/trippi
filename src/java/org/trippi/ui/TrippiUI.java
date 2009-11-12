package org.trippi.ui;

import java.io.File;
import java.io.FileInputStream;

import org.apache.log4j.xml.DOMConfigurator;
import org.trippi.Trippi;
import org.trippi.TrippiException;
import org.trippi.config.TrippiConfig;
import org.trippi.config.TrippiProfile;
import org.trippi.ui.console.TrippiConsole;
import org.trippi.ui.swing.TrippiSwing;

public class TrippiUI {

    private final static String CONFIG_PATH = "config" + File.separator 
                                                       + "trippi.config";
    private final static String LOG_CONFIG_PATH = "config" + File.separator 
                                                           + "log4j.xml";

    public static void printUsageAndExit() {
        System.err.println("Usage: trippi -u");
        System.err.println("   Or: trippi -v");
        System.err.println("   Or: trippi -c [profile]");
        System.err.println("   Or: trippi -b commands.txt");
        System.err.println();
        System.err.println("With no options, Trippi starts in gui mode.");
        System.err.println();
        System.err.println("The -u option prints usage information and exits.");
        System.err.println("The -v option prints the version and exits.");
        System.err.println("The -c option starts in console mode.");
        System.err.println("The -b option starts in batch mode, taking a file as input.");
        System.err.println("       Note: The file must end with 'exit;'");
        System.exit(0);
    }

    public static void main(String[] args) {
        // don't worry about validation if they just want the version
        if (args.length == 1) {
            if (args[0].startsWith("-v")) {
                System.out.println("Trippi v" + Trippi.VERSION);
                System.exit(0);
            } else if (args[0].startsWith("-u")) {
                printUsageAndExit();
            }
        }
        if (args.length > 2) printUsageAndExit();
        try {
            // validate environment setup
            String homePath = System.getProperty("trippi.home");
            if (homePath == null || homePath == "") {
                throw new TrippiException("trippi.home not set.");
            }
            File homeDir = new File(homePath);
            if (!homeDir.exists() || !homeDir.isDirectory()) {
                throw new TrippiException("Bad trippi.home: " + homePath);
            }
            TrippiConfig config = new TrippiConfig(new File(homeDir, CONFIG_PATH));
            File logConfigFile = new File(homeDir, LOG_CONFIG_PATH);
            if (!logConfigFile.exists()) {
                throw new TrippiException("Can't find logging "
                        + "config file: " + logConfigFile.toString());
            }

            // init logging from file
            DOMConfigurator.configure(logConfigFile.toString());

            // validate params, then start up
            if (args.length == 0) {
                new TrippiSwing(config, null);
            } else if (args.length == 1) {
                if (args[0].startsWith("-c")) {
                    new TrippiConsole(config, null);
                } else {
                    TrippiProfile profile = config.getProfiles().get(args[0]);
                    if (profile == null)
                        throw new TrippiException("Non-existent profile: " + args[0]);
                    new TrippiSwing(config, profile);
                }
            } else { // must be 2, -c profile or -b commands.txt
                if (args[0].startsWith("-c")) {
                    TrippiProfile profile = config.getProfiles().get(args[1]);
                    if (profile == null)
                        throw new TrippiException("Non-existent profile: " + args[1]);
                    new TrippiConsole(config, profile);
                } else if (args[0].startsWith("-b")) {
                    FileInputStream in = new FileInputStream(new File(args[1]));
                    try {
                        new TrippiConsole(config, null, in);
                    } finally {
                        in.close();
                    }
                } else {
                    printUsageAndExit();
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
            if (e.getCause() != null) {
                System.err.println("CAUSED BY: " + e.getCause().getClass().getName());
                e.getCause().printStackTrace();
            }
        }
    }

}
