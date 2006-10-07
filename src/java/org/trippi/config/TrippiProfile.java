package org.trippi.config;

import java.util.Iterator;
import java.util.Map;

import org.trippi.TriplestoreConnector;
import org.trippi.TrippiException;

/**
 * Named settings for working with a specific triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
public class TrippiProfile {

    private String m_id;
    private String m_label;
    private String m_connectorClassName;
    private Map m_configuration;

    public TrippiProfile(String id,
                         String label,
                         String connectorClassName,
                         Map configuration) {
        m_id = id;
        m_label = label;
        m_connectorClassName = connectorClassName;
        m_configuration = configuration;
    }

    public String getId() { return m_id; }
    public String getLabel() { return m_label; }
    public String getConnectorClassName() { return m_connectorClassName; }
    public Map getConfiguration() { return m_configuration; }

    public void setLabel(String label) { m_label = label; }

    public TriplestoreConnector getConnector() throws TrippiException,
                                                      ClassNotFoundException {
        return TriplestoreConnector.init(getConnectorClassName(),
                                         getConfiguration());
    }

    public String toString() {
        StringBuffer out = new StringBuffer();
        out.append("Trippi Profile : " + getId() + "\n");
        out.append("         Label : " + getLabel() + "\n");
        out.append("         Class : " + getConnectorClassName() + "\n");
        Map m = getConfiguration();
        Iterator iter = m.keySet().iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            out.append("         Param : " + key + " = " + (String) m.get(key) + "\n");
        }
        return out.toString();
    }

}
