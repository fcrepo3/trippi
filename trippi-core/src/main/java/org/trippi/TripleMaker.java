package org.trippi;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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

    private static RDFUtil m_factory = new RDFUtil();
    
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
        try {
            return m_factory.parse(n);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        } catch (URISyntaxException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    
    ///// printing methods /////

    public static String toString(Node node) {
        return RDFUtil.toString(node);
    }
    

    public static String toString(Triple triple) {
        return RDFUtil.toString(triple);
    }
    
    public static void encode(Node n, Appendable a) throws IOException {
        RDFUtil.encode(n, a);
    }

    public static void encode(Triple t, Appendable a) throws IOException {
        RDFUtil.encode(t, a);
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