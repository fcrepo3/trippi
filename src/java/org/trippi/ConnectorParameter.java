package org.trippi;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * A parameter to a <code>TriplestoreConnector</code>.
 * <p>
 * Instances of this class document the nature of a parameter, the values
 * it may have, and the other parameters that are valid only in the context
 * of a certain value of this one.
 * </p><p>
 * For example, consider the <b>remote</b> parameter to a triplestore,
 * indicating whether it is accessed via the network or the filesystem.
 * In the case that remote is <i>true</i>, the parameters <b>hostname</b>
 * and <b>port</b> would be needed.  In the case where it is <i>false</i>,
 * a <b>path</b> parameter would be needed.
 * </p>
 * @author cwilper@cs.cornell.edu
 */
public class ConnectorParameter {

    private String m_name;
    private String m_label;
    private String m_description;
    private boolean m_isOptional;
    private List m_options;
    private Map m_parameterMap;

    public ConnectorParameter(String name,
                              String label,
                              String description,
                              boolean isOptional,
                              List options,
                              Map parameterMap) {
        m_name = name;
        m_label = label;
        m_description = description;
        m_isOptional = isOptional;
        m_options = options;
        m_parameterMap = parameterMap;
    }



    /** The name of the parameter. */
    public String getName() {
        return m_name;
    }

    /** A simple description of the parameter. */
    public String getLabel() {
        return m_label;
    }

    /** A longer description of the parameter. */
    public String getDescription() {
        return m_description;
    }

    /** Whether the parameter may be unspecified. */
    public boolean isOptional() {
        return m_isOptional;
    }

    /** 
     * Get the value options for this parameter.
     *
     * If the array is non-empty, it can be assumed that it contains
     * all possible valid values for the parameter.  Otherwise, its
     * value will be considered open-ended.
     */
    public List getOptions() {
        return m_options;
    }

    /**
     * For a given value of this parameter, get the list of additional 
     * <code>ConnectorParameter</code>s that should be considered.
     *
     * @return the List of parameters (may be empty).
     */
    public List getParameters(String value) {
        return (List) m_parameterMap.get(value);
    }

    public String toString(int i) {
        StringBuffer out = new StringBuffer();
        out.append(indent(i) + "Parameter      : " + getName() + "\n");
        out.append(indent(i) + " label         : " + getLabel() + "\n");
        out.append(indent(i) + " description   : " + getDescription() + "\n");
        out.append(indent(i) + " is optional   : " + isOptional() + "\n");
        Iterator iter = getOptions().iterator();
        while ( iter.hasNext() ) {
            String val = (String) iter.next();
            out.append(indent(i) + " OPTION        : " + val + "\n");
            List p = getParameters(val);
            if (p != null) {
                Iterator pIter = p.iterator();
                while ( pIter.hasNext() ) {
                    out.append(((ConnectorParameter) pIter.next()).
                                toString(i + 4));
                }
            }
        }
        return out.toString();
    }

    public String toString() {
        return toString(0);
    }

    private static String indent(int by) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < by; i++) out.append(' ');
        return out.toString();
    }

}