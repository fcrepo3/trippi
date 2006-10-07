package org.trippi;

import java.io.InputStream;
import java.util.Map;

import org.jrdf.graph.GraphElementFactory;

/**
 * Provides a <code>TriplestoreReader</code> and/or
 * <code>TriplestoreWriter</code> for a triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
public abstract class TriplestoreConnector {

    /**
     * Instantiate a named TriplestoreConnector.
     */
    public static TriplestoreConnector init(String className,
                                            Map configuration) 
            throws TrippiException,
                   ClassNotFoundException {
        TriplestoreConnector connector = getConnector(className);
        connector.init(configuration);
        return connector;
    }

    public static ConnectorDescriptor getDescriptor(String className) 
            throws TrippiException {
        String path = className.replaceAll("\\.", "/") + "Descriptor.xml";
        InputStream xml = ClassLoader.getSystemClassLoader().
                          getResourceAsStream(path);
        if (xml != null) {
            return new ConnectorDescriptor(xml);
        }
        throw new TrippiException("Not found in classpath: " + path);
    }

    private static TriplestoreConnector getConnector(String className)
            throws TrippiException,
                   ClassNotFoundException {
        Class connectorClass = Class.forName(className);
        TriplestoreConnector connector = null;
        try {
            return (TriplestoreConnector) connectorClass.newInstance();
        } catch (Exception e) {
            throw new TrippiException("Unable to get an instance of "
                    + className, e);
        }
    }

    /**
     * Initialize this connector with the given configuration.
     */
    public abstract void init(Map configuration) throws TrippiException;

    /**
     * Get the reader. 
     */
    public abstract TriplestoreReader getReader();


    /**
     * Get the writer.
     *
     * @throws UnsupportedOperationException if this connector does not support
     *                                       triplestore modification.
     */
    public abstract TriplestoreWriter getWriter();

    public abstract GraphElementFactory getElementFactory();

    /**
     * Release resources.
     */
    public abstract void close() throws TrippiException;

    /**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }

    public static void main(String[] args) throws Exception {
        ConnectorDescriptor d = TriplestoreConnector.getDescriptor(args[0]);
        System.out.println(d.toString());
    }

}
