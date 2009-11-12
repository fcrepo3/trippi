package org.trippi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.jrdf.graph.AbstractBlankNode;
import org.jrdf.graph.BlankNode;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;

public class TripleMaker {

    private static GraphElementFactory m_factory;
    static {
        m_factory = new RDFUtil();
    }
    private static Map<String, Node> m_blankMap = null;
    
    //private static Map<String, String> m_aliasMap;

    public TripleMaker() throws TrippiException {
        
    }

    /**
     * Create a BlankNode given a unique id.
     */
    public BlankNode createResource(int hashCode) {
        return new FreeBlankNode(hashCode);
    }

    /////// org.jrdf.graph.GraphElementFactory ///////

    public static Literal createLiteral(String lexicalValue) 
            throws TrippiException {
        try {
            return m_factory.createLiteral(lexicalValue);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    public static Literal createLiteral(String lexicalValue, String languageType) 
            throws TrippiException {
        try {
            return m_factory.createLiteral(lexicalValue, languageType);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    public static Literal createLiteral(String lexicalValue, URI datatypeURI) 
            throws TrippiException {
        try {
            return m_factory.createLiteral(lexicalValue, datatypeURI);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    public static BlankNode createResource()
            throws TrippiException {
        try {
            return m_factory.createResource();
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }
    
    /**
     * Convenience method
     * @param uri
     * @return URIReference
     * @throws TrippiException
     */
    public static URIReference createResource(String uri) 
            throws TrippiException {
        try {
            return createResource(new URI(uri));
        } catch (URISyntaxException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }
    
    public static URIReference createResource(URI uri) 
            throws TrippiException {
        try {
            return m_factory.createResource(uri);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    public static URIReference createResource(URI uri, boolean validate) 
            throws TrippiException {
        try {
            return m_factory.createResource(uri, validate);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    public static Triple create(SubjectNode subject, 
                                PredicateNode predicate, 
                                ObjectNode object) 
            throws TrippiException {
        try {
            return m_factory.createTriple(subject, predicate, object);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }
    
    public static Triple create(String subject, 
                                String predicate, 
                                String object) 
            throws TrippiException {
        return create(createResource(subject),
                      createResource(predicate),
                      createResource(object));
    }
    
    public static Triple createPlain(String subject, 
                                     String predicate, 
                                     String object) 
            throws TrippiException {
        return create(createResource(subject),
                      createResource(predicate),
                      createLiteral(object));
    }
    
    public static Triple createLocal(String subject, 
                                     String predicate, 
                                     String object, 
                                     String language) 
            throws TrippiException {
        return create(createResource(subject),
                      createResource(predicate),
                      createLiteral(object, language));
    }
    
    public static Triple createTyped(String subject, 
                                     String predicate, 
                                     String object, 
                                     String datatype) throws TrippiException {
        try {
            return create(createResource(subject),
                          createResource(predicate),
                          createLiteral(object, new URI(datatype)));
        } catch (URISyntaxException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }
    
    public static GraphElementFactory getGraphElementFactory() {
        return m_factory;
    }
    
    public static void setAliasMap(Map<String, String> aliasMap) {

    }

    ////// parsing methods //////

    // the node should start with one following: < " ' _
    public static Node parse(String n) 
            throws TrippiException {
        if (m_blankMap == null) m_blankMap = new HashMap<String, Node>();  // lazily
        if (n.length() > 0) {
            char c = n.charAt(0);
            if (c == '<' && n.length() > 4) {   // <a:b>
                try {
                    return createResource(new URI(stripFirstAndLast(n)));
                } catch (URISyntaxException e) {
                    throw new TrippiException(e.getMessage(), e);
                }
            } else if ( ( c == '"' || c == '\'' ) && n.length() > 1) { // ''
                int i = n.lastIndexOf(c);
                if ( i == n.length() - 1 ) {
                    return createLiteral(unescapeLiteral(stripFirstAndLast(n)));
                } else {
                    String uriString = n.substring(1, i);
                    String qualifier = n.substring(i + 1);
                    if (qualifier.startsWith("@")) {
                        return createLiteral(uriString, qualifier.substring(1));
                    } else if (qualifier.startsWith("^^")) {
                        try {
                            return createLiteral(uriString, new URI(qualifier.substring(2)));
                        } catch (URISyntaxException e) {
                            throw new TrippiException(e.getMessage(), e);
                        }
                    } else {
                        throw new TrippiException("Malformed literal: " + n);
                    }
                }
            } else if (c == '_') {
                Node blankNode = m_blankMap.get(n);
                if (blankNode == null) {
                    blankNode = createResource();
                    m_blankMap.put(n, blankNode);
                }
                return blankNode;
            }
        }
        throw new TrippiException("Could not parse as Node: " + n);
    }

    private static String stripFirstAndLast(String s) {
        StringBuffer out = new StringBuffer();
        for (int i = 1; i < s.length() - 1; i++) {
            out.append(s.charAt(i));
        }
        return out.toString();
    }

    private static String escapeLiteral(String s) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ( c == '"' ) {
                out.append("\\\"");
            } else if ( c == '\\' ) {
                out.append("\\\\");
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static String unescapeLiteral(String s) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                char d = s.charAt(++i);
                out.append(d);
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
    
    ///// printing methods /////

    public static String toString(Node node) {
        if (node == null) return "null";
        if (node instanceof URIReference) {
            URIReference n = (URIReference) node;
            return "<" + n.getURI().toString() + ">";
        } else if (node instanceof BlankNode) {
            return "_node" + node.hashCode();
        } else {
            Literal n = (Literal) node;
            StringBuffer out = new StringBuffer();
            out.append("\"" + escapeLiteral(n.getLexicalForm()) + "\"");
            if (n.getLanguage() != null && n.getLanguage().length() > 0) {
                out.append("@" + n.getLanguage());
            } else if (n.getDatatypeURI() != null) {
                out.append("^^" + n.getDatatypeURI().toString());
            }
            return out.toString();
        }
    }

    public static String toString(Triple triple) {
        return toString(triple.getSubject()) + " "
             + toString(triple.getPredicate()) + " "
             + toString(triple.getObject());
    }

    public class FreeBlankNode extends AbstractBlankNode {
        private static final long serialVersionUID = 1L;
        private int m_hashCode;
        public FreeBlankNode(int hashCode) { 
            m_hashCode = hashCode; 
        }
        @Override
		public int hashCode() { 
            return m_hashCode; 
        }
        public String getID() {
            return "node" + Integer.toString(m_hashCode);
        }
    }
}