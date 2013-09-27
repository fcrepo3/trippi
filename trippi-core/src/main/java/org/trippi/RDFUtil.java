package org.trippi;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.jrdf.graph.AbstractBlankNode;
import org.jrdf.graph.AbstractLiteral;
import org.jrdf.graph.AbstractTriple;
import org.jrdf.graph.AbstractURIReference;
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

public class RDFUtil implements GraphElementFactory, java.io.Serializable {

	private static final long serialVersionUID = 1L;
	private Map<String, Node> m_blankMap = null;

    public RDFUtil() {
    }

    ///// printing methods /////

    public static String toString(Node node) {
        if (node == null) return "null";
        StringBuffer out = new StringBuffer(128);
        try {
            encode(node, out);
        } catch (IOException wonthappen) {}
        return out.toString();
    }
    
    public static void encode(Node node, Appendable buffer)
            throws IOException {
        if (node == null) buffer.append("null");
        if (node instanceof URIReference) {
            bracket(node.toString(), buffer);
        } else if (node instanceof BlankNode) {
            buffer.append("_node").append(Integer.toString(node.hashCode()));
        } else {
            Literal n = (Literal) node;
            buffer.append('"');
            escapeLiteral(n.getLexicalForm(), buffer);
            buffer.append('"');
            if (n.getLanguage() != null && n.getLanguage().length() > 0) {
                buffer.append('@').append(n.getLanguage());
            } else if (n.getDatatypeURI() != null) {
                buffer.append("^^").append(n.getDatatypeURI().toString());
            }
        }
    }
    
    private static void bracket(String in, Appendable buffer)
           throws IOException {
        bracket(in, buffer, '<', '>');
    }
    
    private static void bracket(String in, Appendable buffer,
            char open, char close)
            throws IOException {
        buffer.append(open);
        buffer.append(in);
        buffer.append(close);
    }

    public static String toString(Triple triple) {
        StringBuffer buffer = new StringBuffer();
        try {
            encode(triple, buffer);
        } catch (IOException wonthappen) {}
        return buffer.toString();
    }
    
    public static void encode(Triple triple, Appendable buffer)
        throws IOException {
        encode(triple.getSubject(), buffer);
        buffer.append(' ');
        encode(triple.getPredicate(), buffer);
        buffer.append(' ');
        encode(triple.getObject(), buffer);        
    }

    ////// parsing methods //////

    // the node should start with one following: < " ' _
    public Node parse(String n) 
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (m_blankMap == null) m_blankMap = new HashMap<String, Node>();  // lazily
        if (n.length() > 0) {
            char c = n.charAt(0);
            if (c == '<' && n.length() > 4) {   // <a:b>
                return createResource(new URI(stripFirstAndLast(n)));
            } else if ( ( c == '"' || c == '\'' ) && n.length() > 1) { // ''
                int i = n.lastIndexOf(c);
                if ( i == n.length() - 1 ) {
                    return createLiteral(unescapeLiteral(stripFirstAndLast(n)));
                } else {
                    String uriString = n.substring(1, i);
                    String qualifier = n.substring(i + 1);
                    if (qualifier.charAt(0) == '@') {
                        return createLiteral(uriString, qualifier.substring(1));
                    } else if (qualifier.startsWith("^^")) {
                        return createLiteral(uriString, new URI(qualifier.substring(2)));
                    } else {
                        throw new GraphElementFactoryException("Malformed literal: " + n);
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
        throw new GraphElementFactoryException("Could not parse as Node: " + n);
    }

    private static String stripFirstAndLast(String s) {
        return s.substring(1, s.length()-1);
    }
    
    private static void escapeLiteral(String s, Appendable buffer)
            throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ( c == '"' ) {
                buffer.append("\\\"");
            } else if ( c == '\\' ) {
                buffer.append("\\\\");
            } else {
                buffer.append(c);
            }
        }
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

    /**
     * Create a BlankNode given a unique id.
     */
    public BlankNode createResource(int hashCode) {
        return new FreeBlankNode(hashCode);
    }

    /////// org.jrdf.graph.GraphElementFactory ///////

    public Literal createLiteral(String lexicalValue) 
            throws GraphElementFactoryException {
        return new FreeLiteral(lexicalValue);
    }

    public Literal createLiteral(String lexicalValue, String languageType) 
            throws GraphElementFactoryException {
        return new FreeLiteral(lexicalValue, languageType);
    }

    public Literal createLiteral(String lexicalValue, URI datatypeURI) 
            throws GraphElementFactoryException {
        return new FreeLiteral(lexicalValue, datatypeURI);
    }

    public BlankNode createResource()
            throws GraphElementFactoryException {
        return new FreeBlankNode(new Object());
    }

    public URIReference createResource(URI uri) 
            throws GraphElementFactoryException {
        return new FreeURIReference(uri);
    }

    public URIReference createResource(URI uri, boolean validate) 
            throws GraphElementFactoryException {
        return new FreeURIReference(uri, validate);
    }

    @Override
    public Triple createTriple(SubjectNode subject, 
                               PredicateNode predicate, 
                               ObjectNode object) 
            throws GraphElementFactoryException {
        return new FreeTriple(subject, predicate, object);
    }

    public class FreeBlankNode extends AbstractBlankNode {
		private static final long serialVersionUID = 1L;
		private int m_hashCode;
        public FreeBlankNode(int hashCode) { 
            m_hashCode = hashCode;
        }
        public FreeBlankNode(Object object) {
            m_hashCode = object.hashCode();
        }
        @Override
		public int hashCode() { 
            return m_hashCode; 
        }
        public String getID() {
            return "node" + Integer.toString(m_hashCode);
        }
    }

    public class FreeLiteral extends AbstractLiteral {
		private static final long serialVersionUID = 1L;
		public FreeLiteral(String lexicalForm) {
            super(lexicalForm);
        }
        public FreeLiteral(String lexicalForm, String language) {
            super(lexicalForm, language);
        }
        public FreeLiteral(String lexicalForm, URI datatypeURI) {
            super(lexicalForm, datatypeURI);
        }
    }

    public class FreeURIReference extends AbstractURIReference {
		private static final long serialVersionUID = 1L;
		public FreeURIReference(URI uri) {
            super(uri);
        }
        public FreeURIReference(URI uri, boolean validate) {
            super(uri, validate);
        }
    }

    public class FreeTriple extends AbstractTriple {
		private static final long serialVersionUID = 1L;

		public FreeTriple(SubjectNode subjectNode,
                          PredicateNode predicateNode,
                          ObjectNode objectNode) {
            this.subjectNode = subjectNode;
            this.predicateNode = predicateNode;
            this.objectNode = objectNode;
        }
    }

}