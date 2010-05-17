package org.trippi.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TripleIterator;

/**
 * Writes triples as JSON.
 *
 * The N2/Talis spec: http://n2.talis.com/wiki/RDF_JSON_Specification
 * The W3C SPARQL/JSON spec: http://www.w3.org/TR/rdf-sparql-json-res/
 *
 * UTF-8 encoding is used for extended characters.
 */
public class JSONTripleWriter extends TripleWriter {

    private PrintWriter m_out;
    private Map<String, String> m_aliases;
    public JSONTripleWriter(OutputStream out, Map<String, String> aliases) throws TrippiException {
        try {
            m_out = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
            m_aliases = aliases;
        } catch (IOException e) {
            throw new TrippiException("Error setting up writer", e);
        }
    }

    public int write(TripleIterator iter) throws TrippiException {
        	String callback = iter.getCallback();
        	if (callback != null){
                m_out.print(callback);
                m_out.print('(');
        	}
            m_out.println("({\"results\":[");
            int count = 0;
            while (iter.hasNext()) {
            	if (count > 0) m_out.print(',');
            	Triple result = iter.next();
                m_out.println(toJSON(result));
                count++;
            }
            m_out.print("]}");
        	if (callback != null){
                m_out.print(')');
        	}
            m_out.flush();
            iter.close();
            return count;
    }
    
    private final static String JSON_TEMPLATE = "{\"%\" : {\"%\" : [ % ] } }";
    
    /**
     * http://n2.talis.com/wiki/RDF_JSON_Specification
     * @param triple
     * @return JSON serialization of the triple
     */
    public static String toJSON(Triple triple) {
    	String [] parts = JSON_TEMPLATE.split("%");
    	String subject = triple.getSubject().stringValue();
    	String predicate = triple.getPredicate().stringValue();
    	String object = toJSON(triple.getObject());
    	StringBuffer result = new StringBuffer((JSON_TEMPLATE.length() - 3) + subject.length() + predicate.length() + object.length());
    	result.append(parts[0]);
    	result.append(subject);
    	result.append(parts[1]);
    	result.append(predicate);
    	result.append(parts[2]);
    	result.append(object);
    	result.append(parts[3]);
    	return result.toString();
    }
    
    private static String toJSON(ObjectNode node){
    	StringBuffer result = new StringBuffer();
    	result.append('{');
    	result.append("\"type\":");
    	if (node.isLiteral()){
    		result.append("\"literal\"");
    	}
    	else if(node.isURIReference()){
    		result.append("\"uri\"");
    	}
    	else {
    		result.append("\"bnode\"");
    	}
    	result.append(", \"value\":");
    	result.append('"');
    	result.append(node.stringValue().replaceAll("\"", "\\\\\""));
    	result.append('"');
    	if(node.isLiteral()){
        	result.append(", \"datatype\":");
        	result.append('"');
    		((Literal)node).getDatatypeURI();
        	result.append('"');
    	}
    	result.append('}');
    	return result.toString();
    }

    public String getValue(Node node) {
        String fullString = RDFUtil.toString(node);
        if (m_aliases != null) {
            if (node instanceof URIReference) {
                Iterator<String> iter = m_aliases.keySet().iterator();
                while (iter.hasNext()) {
                    String alias = (String) iter.next();
                    String prefix = (String) m_aliases.get(alias);
                    if (fullString.startsWith("<" + prefix)) {
                        return fix("<" + alias + ":" + fullString.substring(prefix.length() + 1));
                    }
                }
            } else if (node instanceof Literal) {
                Literal literal = (Literal) node;
                if (literal.getDatatypeURI() != null) {
                    String uri = literal.getDatatypeURI().toString();
                    Iterator<String> iter = m_aliases.keySet().iterator();
                    while (iter.hasNext()) {
                        String alias = iter.next();
                        String prefix = m_aliases.get(alias);
                        if (uri.startsWith(prefix)) {
                            StringBuffer out = new StringBuffer();
                            out.append('"');
                            out.append(literal.getLexicalForm().replaceAll("\"", "\\\""));
                            out.append("\"^^");
                            out.append(alias);
                            out.append(':');
                            out.append(uri.substring(prefix.length()));
                            return fix(out.toString());
                        }
                    }
                }
            }
        }
        return fix(fullString);
    }

    private String fix(String in) {
        if (in.startsWith("\"")) {
            // literal
            String lit = in.substring(1, in.lastIndexOf("\""));
            return lit.replaceAll("\\\\\"", "\"").replaceAll("\n", " ");
        } else if (in.startsWith("<")) {
            // resource
            return in.substring(1, in.length() - 1);
        } else {
            return in;
        }
    }

}
