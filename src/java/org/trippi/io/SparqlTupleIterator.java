package org.trippi.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jrdf.graph.Node;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

/**
 * Deserializes sparql results while iterating.
 *
 * http://www.w3.org/2001/sw/DataAccess/rf1/result
 */
public class SparqlTupleIterator extends TupleIterator {

    private InputStream m_inputStream;
    private XmlPullParser m_xpp;
    private String[] m_names;
    private Map m_next;
    private boolean m_closed;
    private RDFUtil m_util;

    public SparqlTupleIterator(InputStream in) throws TrippiException {
        try {
            m_inputStream = in;
            m_xpp = XmlPullParserFactory.newInstance().newPullParser();
            m_xpp.setInput(new InputStreamReader(m_inputStream));
            m_xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, false);
            m_closed = false;
            m_util = new RDFUtil();
            m_names = parseNames();
            m_next = getNext();
        } catch (IOException e) {
            throw new TrippiException("IOError with xml stream", e);
        } catch (XmlPullParserException e) {
            throw new TrippiException("Error parsing", e);
        }
    }

    public boolean hasNext() {
        return (m_next != null);
    }

    public Map next() throws TrippiException {
        if (m_next == null) return null;
        Map last = m_next;
        m_next = getNext();
        return last;
    }

    private Map getNext() throws TrippiException {
        try {
            // parse until <result>  (return null if none seen till end of doc)
            boolean inResult = false;
            while (!inResult) {
                int eventType = m_xpp.next();
                if (eventType == XmlPullParser.START_TAG && m_xpp.getName().equals("result")) {
                    inResult = true;
                } else if (eventType == XmlPullParser.END_DOCUMENT) {
                    return null;
                }
            }
            Map m = new HashMap();
            while (inResult) {
                int eventType = m_xpp.next();
                if (eventType == XmlPullParser.START_TAG) {
                    m.put(m_xpp.getName(), parseNode());
                } else if (eventType == XmlPullParser.END_TAG) {
                    inResult = false;
                }
            }
            return m;
        } catch (IOException e) {
            throw new TrippiException("IO Error while getting next result", e);
        } catch (XmlPullParserException e) {
            throw new TrippiException("Parser error while getting next result", e);
        }
    }

    private Node parseNode() throws TrippiException {
        try {
            // check for href, datatype, xml:lang, nodeID attribs, and value
            String uri = m_xpp.getAttributeValue(null, "uri");
            if (uri != null) {
                // resource
                m_xpp.nextText();
                return m_util.createResource(new URI(uri));
            } else {
                String dType = m_xpp.getAttributeValue(null, "datatype");
                if (dType != null) {
                    // typed literal
                    return m_util.createLiteral(m_xpp.nextText(), new URI(dType));
                } else {
                    String lang = m_xpp.getAttributeValue(null, "xml:lang");
                    if (lang != null) {
                        // local literal
                        return m_util.createLiteral(m_xpp.nextText(), lang);
                    } else {
                        String nodeID = m_xpp.getAttributeValue(null, "bnodeid");
                        if (nodeID != null) {
                            // blank node
                            m_xpp.nextText();
                            return m_util.createResource(nodeID.hashCode());
                        } else {
                            String bound = m_xpp.getAttributeValue(null, "bound");
                            if (bound != null && bound.equals("false")) {
                                // unbound
                                m_xpp.nextText();
                                return null;
                            }
                            // plain literal
                            String val = m_xpp.nextText();
                            if (val == null) val = "";
                            return m_util.createLiteral(val);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new TrippiException("Error parsing value as JRDF Node.", e);
        }
    }

    private String[] parseNames() throws XmlPullParserException, 
                                         IOException {
        boolean inVariables = false;
        // scan till we reach variables
        while (!inVariables) {
            int eventType = m_xpp.next();
            if (eventType == XmlPullParser.START_TAG && m_xpp.getName().equals("head")) {
                inVariables = true;
            }
        }
        // put each in List
        List names = new ArrayList();
        while (inVariables) {
            int eventType = m_xpp.next();
            if (eventType == XmlPullParser.START_TAG) {
                names.add(m_xpp.getAttributeValue(null, "name"));
            } else if (eventType == XmlPullParser.END_TAG && m_xpp.getName().equals("head")) {
                inVariables = false;
            }
        }
        return (String[]) names.toArray(new String[0]);
    }

    public String[] names() {
        return m_names;
    }

    public void close() throws TrippiException {
        if (!m_closed) {
            try {
                m_inputStream.close();
                m_closed = true;
            } catch (Exception e) {
                throw new TrippiException("Error closing underlying InputStream.", e);
            }
        }
    }

}


