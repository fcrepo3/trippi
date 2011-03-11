package org.trippi.ui.console;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jrdf.graph.Triple;
import org.trippi.ConnectorDescriptor;
import org.trippi.ConnectorParameter;
import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TripleMaker;
import org.trippi.TriplePattern;
import org.trippi.TriplestoreConnector;
import org.trippi.Trippi;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.config.TrippiConfig;
import org.trippi.config.TrippiProfile;

public class TrippiConsole {

    private static final Logger logger =
        LoggerFactory.getLogger(TrippiConsole.class.getName());

    private TrippiConfig m_config;

    private boolean m_trace = false;

    private TrippiProfile m_profile;
    private TriplestoreConnector m_connector;

    private BufferedReader m_reader;

    private boolean m_distinct = false;
    private int m_limit = 0;
    private RDFFormat m_tupleForm = RDFFormat.TSV;
    private RDFFormat m_tripleForm = RDFFormat.TURTLE;

    /**
     * Start in interactive mode.
     */
    public TrippiConsole(TrippiConfig config, 
                         TrippiProfile profile) throws Exception {
        this(config, profile, System.in);
    }

    public TrippiConsole(TrippiConfig config, 
                         TrippiProfile profile,
                         InputStream in) throws Exception {
        m_config = config;
        m_reader = new BufferedReader(new InputStreamReader(in));
        System.out.println("Welcome to the Trippi v" + Trippi.VERSION + " Console.");
        if (profile == null) {
            System.out.println("You are not connected to a triplestore.");
        } else {
            connect(profile);
        }
        System.out.println("\nType 'help;' for help.\n");
        boolean done = false;
        while (!done) {
            done = inputAndRun();
        }
        if (m_connector != null) {
            m_connector.close();
        }
    }

    private void connect(TrippiProfile profile) {
        System.out.print("Connecting to " + profile.getLabel() + "...");
        try {
            m_connector = profile.getConnector();
            m_connector.getReader().setAliasMap(m_config.getAliasMap());
            m_profile = profile;
            System.out.println("OK");
        } catch (Exception e) {
            System.out.println("\nERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean inputAndRun() throws IOException {
        String prompt;
        if (m_profile == null) {
            prompt = "trippi> ";
        } else {
            prompt = "trippi/" + m_profile.getId() + "> ";
        }
        System.out.print(prompt);
        // input until trailing ; is reached
        StringBuffer input = new StringBuffer();
        String line;
        boolean finished = false;
        boolean done = false;
        while (!done) {
            line = m_reader.readLine();
            if (line == null) {
                done = true; finished = true;
            } else {
                line = line.trim();
                if (line.endsWith(";")) {
                    input.append((line.substring(0, line.length() - 1)).trim());
                    done = true;
                } else {
                    input.append(line + " ");
                    System.out.print("> ");
                }
            }
        }
        if (finished) return true;
        // now look at the command and interpret it
        String cmdLine = input.toString().trim();
        String u = cmdLine.toUpperCase();
        if ( u.startsWith("EXIT") || u.startsWith("QUIT")) {
            return true;
        } else {
            try {
                System.out.println("");
                String[] tokens = cmdLine.split(" ");
                if (tokens == null || tokens.length < 2) {
                    interpret(cmdLine.toLowerCase(), "");
                } else {
                    interpret(tokens[0].toLowerCase(), cmdLine.substring(tokens[0].length() + 1));
                }
            } catch (Exception e) {
                System.out.println("ERROR: " + e.getMessage());
                if (m_trace) {
                    printFullTrace(e);
                }
            } finally {
                System.out.println("");
            }
            return false;
        }
    }

    private void printFullTrace(Throwable e) {
        int level = 0;
        while (e != null) {
            if (level > 0) System.out.println("CAUSED BY:");
            level++;
            e.printStackTrace();
            e = e.getCause();
        }
    }

    public void doDateTest(String cmd) throws Exception {
        if (cmd == null || cmd.equals("")) {
            System.out.println("Usage: datetest [plain|double|dateTime] [numTriples]");
        } else {
            String[] parts = cmd.split(" ");
            if (parts.length == 2) {
                int num = Integer.parseInt(parts[1]); 
                int type;
                if (parts[0].equals("plain")) {
                    type = 0;
                } else if (parts[0].equals("double")) {
                    type = 1;
                } else if (parts[0].equals("dateTime")) {
                    type = 2;
                } else {
                    throw new TrippiException("Unrecognized datetest type: " + parts[0]);
                }
                System.out.print("Adding " + num + " <resource:NUM> <urn:date> (" + parts[0] + ") triples...");
                long startTime = System.currentTimeMillis();
                String property = "urn:date";
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                for (int i = 0; i < num; i++) {
                    String resource = "resource:" + i;
                    Triple triple;
                    if (type == 1) {
                        triple = TripleMaker.createTyped(resource,
                                                           property,
                                                           "" + i,
                                                           "http://www.w3.org/2001/XMLSchema#double");
                    } else {
                        long epochMS = (long) (i * 1000);
                        Date date = new Date();
                        date.setTime(epochMS);
                        String dateString = formatter.format(date);
                        if (type == 0) {
                            triple = TripleMaker.createPlain(resource,
                                                               property,
                                                               dateString);
                        } else {
                            triple = TripleMaker.createTyped(resource,
                                                               property,
                                                               dateString,
                                                               "http://www.w3.org/2001/XMLSchema#dateTime");
                        }
                    }
                    m_connector.getWriter().add(triple, false);
                }
                System.out.println("OK.");
                long endTime = System.currentTimeMillis();
                double elapsed = ((double) (endTime - startTime)) / 1000.0;
                System.out.println(elapsed + " seconds elapsed.");
            } else {
                System.out.println("Usage: datetest [plain|double|dateTime] [numTriples]");
            }
        }
    }

    public void interpret(String cmd, String rest) throws Exception {
        if (cmd.equals("help")) {
            doHelp();
        } else if (cmd.equals("profiles")) {
            doProfiles();
        } else if (cmd.equals("connectors")) {
            doConnectors();
        } else if (cmd.equals("use")) {
            doUse(rest);
        } else if (cmd.equals("create")) {
            doCreate();
        } else if (cmd.equals("alias")) {
            doAlias(rest);
        } else if (cmd.equals("datetest")) {
            checkProfile();
            doDateTest(rest);
        } else if (cmd.equals("add")) {
            checkProfile();
            doAdd(rest);
        } else if (cmd.equals("delete")) {
            checkProfile();
            doDelete(rest);
        } else if (cmd.equals("tuples")) {
            checkProfile();
            doTuples(rest, false);
        } else if (cmd.equals("count")) {
            checkProfile();
            if (rest.startsWith("tuples")) {
                doTuples(rest.substring(7), true);
            } else if (rest.startsWith("triples")) {
                doTriples(rest.substring(8), true);
            } else if (rest.equals("")) {
                doTriples("spo * * *", true);
            } else {
                throw new TrippiException("Bad syntax, try count by itself or count [tuples ..|triples ..]");
            }
        } else if (cmd.equals("triples")) {
            checkProfile();
            doTriples(rest, false);
        } else if (cmd.equals("dump")) {
            checkProfile();
            doDump(rest);
        } else if (cmd.startsWith("triplef")) {
            if (rest.equals("")) {
                System.out.println("Current Triple Result Format: " + m_tripleForm.getName());
                System.out.print("Formats Supported: ");
                RDFFormat[] p = TripleIterator.OUTPUT_FORMATS;
                for (int i = 0; i < p.length; i++) {
                    if (i > 0) System.out.print(", ");
                    System.out.print(p[i].getName());
                }
                System.out.println("");
            } else {
                m_tripleForm = RDFFormat.forName(rest);
                System.out.println("New Triple Result Format: " + m_tripleForm.getName());
            }
        } else if (cmd.startsWith("tuplef")) {
            if (rest.equals("")) {
                System.out.println("Current Tuple Result Format: " + m_tupleForm.getName());
                System.out.print("Formats Supported: ");
                RDFFormat[] p = TupleIterator.OUTPUT_FORMATS;
                for (int i = 0; i < p.length; i++) {
                    if (i > 0) System.out.print(", ");
                    System.out.print(p[i].getName());
                }
                System.out.println("");
            } else {
                m_tupleForm = RDFFormat.forName(rest);
                System.out.println("New Tuple Result Format: " + m_tupleForm.getName());
            }
        } else if (cmd.equals("load")) {
            checkProfile();
            doLoad(rest);
        } else if (cmd.equals("close")) {
            checkProfile();
            System.out.print("Closing...");
            m_connector.close();
            System.out.println("OK.");
            m_connector = null;
            m_profile = null;
        } else if (cmd.equals("limit")) {
            doLimit(rest);
        } else if (cmd.equals("distinct")) {
            if (m_distinct) {
                m_distinct = false;
                System.out.println("Forced distinct mode is off.");
            } else {
                m_distinct = true;
                System.out.println("Forced distinct mode is on.");
            }
        } else if (cmd.equals("trace")) {
            if (m_trace) {
                m_trace = false;
                System.out.println("Tracing is disabled.");
            } else {
                m_trace = true;
                System.out.println("Tracing is enabled.");
            }
        } else {
            throw new TrippiException("Unrecognized command: " + cmd);
        }
    }

    private static String doAliasReplacements(String string, Map<String, String> aliasMap) {
        String out = string;
        Iterator<String> iter = aliasMap.keySet().iterator();
        while (iter.hasNext()) {
            String alias = iter.next();
            String fullForm = aliasMap.get(alias);
            out = out.replaceAll("<" + alias + ":", "<" + fullForm);
            out = out.replaceAll("\\^\\^" + alias + ":", "^^" + fullForm);
        }
        if (!string.equals(out)) {
            logger.info("Substituted aliases, string is now: " + out);
        }
        return out;
    }

    public void doAdd(String tripleString) throws Exception {
        // first replace aliases in triples
        List<Triple> triples = TriplePattern.parse(
                doAliasReplacements(tripleString,
                m_connector.getReader().getAliasMap()),
                m_connector.getElementFactory());
        System.out.print("Adding " + triples.size() + " triples...");
        m_connector.getWriter().add(triples, false);
        System.out.println("OK");
    }

    public void doDelete(String tripleString) throws Exception {
        if (tripleString.equals("*") || tripleString.equalsIgnoreCase("all")) {
            System.out.print("Deleting ALL triples...");
            TripleIterator iter = m_connector.getReader().findTriples(null, null, null, -1);
            try {
                m_connector.getWriter().delete(iter, false);
            } finally {
                iter.close();
            }
        } else {
            // first replace aliases in triples
            List<Triple> triples = TriplePattern.parse(
                    doAliasReplacements(tripleString,
                    m_connector.getReader().getAliasMap()),
                    m_connector.getElementFactory());
            System.out.print("Deleting " + triples.size() + " triples...");
            m_connector.getWriter().delete(triples, false);
        }
        System.out.println("OK");
    }

    public void doLimit(String rest) throws Exception {
        if (rest.equals("")) {
            if (m_limit < 1) {
                System.out.println("Current result limit: None");
            } else {
                System.out.println("Current result limit: " + m_limit);
            }
        } else {
            m_limit = Integer.parseInt(rest);
            if (m_limit < 1) {
                System.out.println("New result limit: None");
            } else {
                System.out.println("New result limit: " + m_limit);
            }
        }
    }

    public void doAlias(String rest) throws Exception {
        Map<String, String> aliases = m_config.getAliasMap();
        if (rest.equals("")) {
            // show value of all aliases
            Iterator<String> iter = aliases.keySet().iterator();
            while (iter.hasNext()) {
                String alias = iter.next();
                String value = aliases.get(alias);
                System.out.println(alias + "\t" + value);
            }
        } else if (rest.equals("!")) {
            // clear all aliases
            Map<String, String> emptyMap = new HashMap<String, String>();
            m_config.setAliasMap(emptyMap);
            if (m_connector != null) 
                    m_connector.getReader().setAliasMap(emptyMap);
            System.out.println("Cleared all aliases.");
        } else if (rest.indexOf(" ") == -1) {
            // show value of one alias
            String val = (String) aliases.get(rest);
            if (val == null) {
                throw new TrippiException("No such alias: " + rest);
            } else {
                System.out.println(rest + "\t" + val);
            }
        } else {
            String[] tokens = rest.split(" ");
            if (tokens.length != 2) {
            } else if (tokens[1].equals("!")) {
                // clear one alias
                aliases.remove(tokens[0]);
                m_config.setAliasMap(aliases);
                if (m_connector != null) 
                        m_connector.getReader().setAliasMap(aliases);
                System.out.println("Cleared alias: " + tokens[0]);
            } else {
                // set an alias
                aliases.put(tokens[0], tokens[1]);
                m_config.setAliasMap(aliases);
                if (m_connector != null) 
                        m_connector.getReader().setAliasMap(aliases);
                System.out.println(tokens[0] + " is now an alias for " + tokens[1]);
            }
        }
    }

    public void checkProfile() throws Exception {
        if (m_profile == null) 
            throw new TrippiException("No profile in use.");
    }

    public void doDump(String filename) throws Exception {
        System.out.print("Dumping triples (in " + m_tripleForm.getName() + " format) to " + filename + "...");
        TripleIterator iter = m_connector.getReader().findTriples(null, null, null, 0);
        iter.setAliasMap(m_connector.getReader().getAliasMap());
        FileOutputStream out = new FileOutputStream(new File(filename));
        try {
            iter.toStream(out, m_tripleForm);
        } finally {
            out.close();
        }
        System.out.println("OK");
    }

    public void doLoad(String filename) throws Exception {
        System.out.print("Loading triples (in " + m_tripleForm.getName() + " format) from " + filename + "...");
        m_connector.getWriter()
                   .add(TripleIterator.fromStream(
                                        new FileInputStream(new File(filename)), 
                                        m_tripleForm), false);
        System.out.println("OK");
    }

    public void doTuples(String parms, boolean count) throws Exception {
        // parse it first
        String[] tokens = parms.split(" ");
        if (tokens.length < 2) {
            throw new TrippiException("No query specified.");
        }
        String lang = tokens[0];
        String query = parms.substring(parms.indexOf(" ")).trim();
        TupleIterator iter = null;
        try {
            long startTime = System.currentTimeMillis();
            iter = m_connector.getReader().findTuples(lang, query, m_limit, m_distinct);
            int n = 0;
            if (count) {
                n = iter.count();
            } else {
                iter.setAliasMap(m_connector.getReader().getAliasMap());
                n = iter.toStream(System.out, m_tupleForm);
            }
            System.out.println("Total Tuples   : " + n);
            long endTime = System.currentTimeMillis();
            double elapsedSeconds = ((double) endTime - startTime) / 1000.0;
            System.out.println("Seconds Elapsed: " + elapsedSeconds);
        } finally {
            if (iter != null) iter.close();
        }
    }

    public void doTriples(String parms, boolean count) throws Exception {
        TripleIterator iter = null;
        try {
            int rightPos = parms.indexOf(")");
            long startTime = System.currentTimeMillis();
            if (parms.startsWith("(") && rightPos != -1) {
                // do a tuples-to-triples query
                String pattern = parms.substring(0, rightPos);
                String rest = parms.substring(rightPos+1).trim();
                String[] t = rest.split(" ");
                if (t.length < 2) {
                    throw new TrippiException("Bad syntax, need lang and query.");
                }
                String lang = t[0];
                String query = rest.substring(rest.indexOf(" ")).trim();
                iter = m_connector.getReader().findTriples(lang, query, pattern, m_limit, m_distinct);
            } else {
                // do a real triple query (lang query)
                String[] tokens = parms.split(" ");
                if (tokens.length < 2) {
                    throw new TrippiException("Triple query must have at least two parameters.");
                }
                String lang = tokens[0];
                String query = parms.substring(parms.indexOf(" ")).trim();
                iter = m_connector.getReader().findTriples(lang, query, m_limit, m_distinct);
            }
            doTriples(iter, count, startTime);
        } finally {
            if (iter != null) iter.close();
        }

    }

    private void doTriples(TripleIterator iter, boolean count, long startTime) throws Exception {
        int n = 0;
        if (count) {
            n = iter.count();
        } else {
            iter.setAliasMap(m_connector.getReader().getAliasMap());
            n = iter.toStream(System.out, m_tripleForm);
            System.out.println("\n");
        }
        System.out.println("Total Triples  : " + n);
        long endTime = System.currentTimeMillis();
        double elapsedSeconds = ((double) endTime - startTime) / 1000.0;
        System.out.println("Seconds Elapsed: " + elapsedSeconds);
    }

    public void doCreate() throws Exception {
        System.out.println("Creating a new profile. To cancel at any time, enter CANCEL.\n");
        boolean done = false;
        boolean canceled = false;
        String className = null;
        ConnectorDescriptor descriptor = null;
        while (!done) {
            System.out.print("Enter the connector class name: ");
            className = m_reader.readLine();
            if (className == null || className.equalsIgnoreCase("CANCEL")) {
                canceled = true; done = true;
            } else if (!className.equals("")) {
                try {
                    descriptor = TriplestoreConnector.getDescriptor(className);
                    done = true;
                } catch (TrippiException e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
        }
        if (canceled) {
            System.out.println("Canceled.");
        } else {
            String id = null;
            done = false; canceled = false;
            while (!done) {
                System.out.print("Enter a short id for the new profile: ");
                id = m_reader.readLine();
                if (id == null || id.equalsIgnoreCase("CANCEL")) {
                    canceled = true; done = true;
                } else if (!id.equals("")) {
                    TrippiProfile p = (TrippiProfile) m_config.getProfiles().get(id);
                    if (p != null) {
                        System.out.println("ERROR: A profile with that id already exists.");
                    } else {
                        done = true;
                    }
                }
            }
            if (canceled) {
                System.out.println("Canceled.");
            } else {
                String label = null;
                done = false; canceled = false;
                while (!done) {
                    System.out.print("Enter a label for the new profile: ");
                    label = m_reader.readLine();
                    if (label == null || label.equalsIgnoreCase("CANCEL")) {
                        canceled = true; done = true;
                    } else if (!label.equals("")) {
                        done = true;
                    }
                }
                if (canceled) {
                    System.out.println("Canceled.");
                } else {
                    TrippiProfile profile = configure(id, label, className, descriptor);
                    if (profile != null) {
                        Map<String, TrippiProfile> profileMap = m_config.getProfiles();
                        profileMap.put(id, profile);
                        m_config.setProfiles(profileMap);
                        System.out.println("\nProfile created.  Type 'use " + id + ";' to use it.");
                    } else {
                        System.out.println("Canceled.");
                    }
                }
            }
        }
    }

    public TrippiProfile configure(String id,
                                   String label,
                                   String className,
                                   ConnectorDescriptor descriptor) throws Exception {
        System.out.println("");
        printConnectorInfo(descriptor, className);
        Map<String, String> config = configure(descriptor.getParameters());
        if (config == null) return null;
        return new TrippiProfile(id, label, className, config);
    }

    private Map<String, String> configure(List<ConnectorParameter> params) throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        Iterator<ConnectorParameter> iter = params.iterator();
        while (iter.hasNext()) {
            Map<String, String> m = configure(iter.next());
            if (m == null) return null;
            map.putAll(m);
        }
        return map;
    }

    private Map<String, String> configure(ConnectorParameter param) throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        System.out.println("");
        System.out.println("Parameter   : " + param.getName());
        String desc      = "Description : ";
        String blank     = "              ";
        System.out.println(desc + formatString(param.getLabel(),
                                               79 - desc.length(),
                                               79,
                                               desc.length()));
        if (param.getDescription() != null && !param.getDescription().equals("")) {
            System.out.println(blank + formatString(param.getDescription(),
                                                    79 - desc.length(),
                                                    79,
                                                    desc.length()));
        }
        System.out.println("");
        List<String> options = param.getOptions();
        if (options.size() == 0) {
            boolean done = false;
            String input = null;
            while (!done) {
                System.out.print("Enter a value");
                if (param.isOptional()) {
                    System.out.print(" (or [ENTER] for none)");
                }
                System.out.print(": ");
                input = m_reader.readLine();
                if (input == null || input.equalsIgnoreCase("CANCEL")) return null;
                if (input.equals("") && param.isOptional()) {
                    done = true;
                }
                if (!input.equals("")) {
                    done = true;
                }
            }
            if (!input.equals("")) map.put(param.getName(), input);
        } else {
            HashMap<String, List<ConnectorParameter>> o = new HashMap<String, List<ConnectorParameter>>();
            Iterator<String> iter = options.iterator();
            StringBuffer optString = new StringBuffer();
            int i = 0;
            while (iter.hasNext()) {
                i++;
                String val = iter.next();
                o.put(val, param.getParameters(val));
                if (i == options.size()) {
                    optString.append(" or ");
                } else if (i > 1) {
                    optString.append(", ");
                }
                optString.append(val);
            }
            boolean done = false;
            String input = null;
            while (!done) {
                System.out.print("Enter " + optString);
                if (param.isOptional()) {
                    System.out.print(" (or [ENTER] for none)");
                }
                System.out.print(": ");
                input = m_reader.readLine();
                if (input == null || input.equalsIgnoreCase("CANCEL")) return null;
                if (input.equals("") && param.isOptional()) {
                    done = true;
                } else if (o.containsKey(input)) {
                    done = true;
                }
            }
            if (!input.equals("")) {
                // input was ok, so set value in map and move on
                map.put(param.getName(), input);
                List<ConnectorParameter> subs = o.get(input);
                if (subs != null) {
                    Map<String, String> subMap = configure(subs);
                    if (subMap == null) return null;
                    map.putAll(subMap);
                }
            }
        }
        return map;
    }

    public void doConnectors() throws Exception {
        Map<String, ConnectorDescriptor> m = ConnectorDescriptor.find();
        Iterator<String> iter = m.keySet().iterator();
        int i = 0;
        while (iter.hasNext()) {
            if (i > 0) System.out.println("");
            i++;
            String className = iter.next();
            ConnectorDescriptor d = m.get(className);
            printConnectorInfo(d, className);
        }
    }

    private void printConnectorInfo(ConnectorDescriptor d, String className) {
        System.out.println("Connector   : " + d.getName());
        System.out.println("Class       : " + className);
        String desc = "Description : ";
        System.out.println(desc + formatString(d.getDescription(), 
                                               79 - desc.length(),
                                               79,
                                               desc.length()));
    }

    public void doProfiles() throws TrippiException {
        Map<String, TrippiProfile> map = m_config.getProfiles();
        System.out.println("List of all known connection profiles (" + map.keySet().size() + "):");
        System.out.println("");
        Iterator<String> iter = map.keySet().iterator();
        while (iter.hasNext()) {
            TrippiProfile p = map.get(iter.next());
            System.out.println(p.getId() + "\t" + p.getLabel());
        }
    }

    public void doUse(String profileId) throws Exception {
        TrippiProfile profile = (TrippiProfile) m_config.getProfiles().get(profileId);
        if (profile == null) {
            throw new TrippiException("No such profile: '" + profileId + "'");
        } else {
            connect(profile);
        }
    }

    public void doHelp() {
        System.out.println("List of all Trippi Console commands:");
        System.out.println("   (Commands must appear first on line and end with ';')");
        System.out.println("");
        System.out.println("add [triples] . . . . . . . Add one or more triples.");
        System.out.println("alias . . . . . . . . . . . List all alias values.");
        System.out.println("alias [name]  . . . . . . . List an alias value.");
        System.out.println("alias ! . . . . . . . . . . Clear all aliases.");
        System.out.println("alias [name] [val]  . . . . Assign an alias.");
        System.out.println("alias [name] !  . . . . . . Clear an alias.");
        System.out.println("count . . . . . . . . . . . Get the total number of triples.");
        System.out.println("count [triples] . . . . . . Get the number of triples returned by a query.");
        System.out.println("count [tuples]  . . . . . . Get the number of tuples returned by a query.");
        System.out.println("create  . . . . . . . . . . Create a new profile.");
        System.out.println("delete [triples] . .  . . . Delete one or more triples.");
        System.out.println("distinct . . . . . .  . . . Toggle forced distinct mode.");
        System.out.println("dump [file] . . . . . . . . Dump all triples to an RDF/XML file.");
        System.out.println("help  . . . . . . . . . . . Display this help.");
        System.out.println("close . . . . . . . . . . . Close the current connector.");
        System.out.println("connectors  . . . . . . . . List known triplestore connectors.");
        System.out.println("limit . . . . . . . . . . . Show current result limit (default = none)");
        System.out.println("limit [num] . . . . . . . . Set result limit ( 0 means none )");
        System.out.println("load [file] . . . . . . . . Load the RDF/XML file into the triplestore.");
        System.out.println("profiles  . . . . . . . . . List known profiles.");
        System.out.println("trace . . . . . . . . . . . Toggle stack trace printing for errors.");
        System.out.println("triples [pat] [lng] [qry] . Query for tuples.");
        System.out.println("triples [lng] [qry] . . . . Query for tuples.");
        System.out.println("tripleform  . . . . . . . . Show the current triple result format.");
        System.out.println("tripleform [newform]  . . . Switch to a new triple result format.");
        System.out.println("tupleform . . . . . . . . . Show the current tuple result format.");
        System.out.println("tupleform [newform]. .  . . Switch to a new tuple result format.");
        System.out.println("tuples [lng] [qry]  . . . . Query for tuples.");
        System.out.println("use [id]  . . . . . . . . . Use an existing profile.");
    }

    public static String formatString(String in, int firstMax, int otherMax, int otherIndent) {
        StringBuffer out = new StringBuffer();
        String[] tokens = in.split(" ");
        int charsLeft = firstMax;
        int i = 0;
        while ( i < tokens.length ) {
            String token = tokens[i++];
            out.append(token);
            charsLeft -= token.length();
            if (i < tokens.length && tokens[i].length() >= charsLeft) {
                out.append("\n" + indent(otherIndent));
                charsLeft = otherMax - otherIndent;
            } else {
                out.append(' ');
                charsLeft--;
            }
        }
        return out.toString();
    }

    public static String indent(int num) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < num; i++) out.append(' ');
        return out.toString();
    }
}
