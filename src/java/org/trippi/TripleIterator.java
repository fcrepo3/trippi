package org.trippi;

import gnu.trove.TIntObjectHashMap;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.GraphException;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.openrdf.rio.n3.N3Writer;
import org.openrdf.rio.ntriples.NTriplesWriter;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.rio.turtle.TurtleWriter;
import org.trippi.io.CountTripleWriter;
import org.trippi.io.FormatCountTripleWriter;
import org.trippi.io.JSONTripleWriter;
import org.trippi.io.RIOTripleIterator;
import org.trippi.io.RIOTripleWriter;
import org.trippi.io.SparqlTupleWriter;
import org.trippi.io.TripleWriter;
import org.trippi.io.XMLDeclarationRemover;

/**
 * An iterator over a series of <code>Triple</code> objects.
 *
 * @author cwilper@cs.cornell.edu
 */
public abstract class TripleIterator {

    /** 
     * Formats supported for reading.
     *
     * @see #fromStream(InputStream, String, RDFFormat)
     */
    public static final RDFFormat[] INPUT_FORMATS = 
                                         new RDFFormat[] { RDFFormat.N_TRIPLES,
                                                           RDFFormat.RDF_XML,
                                                           RDFFormat.TURTLE };

    /** 
     * Formats supported for writing.
     *
     * @see #toStream(OutputStream, RDFFormat)
     */
    public static final RDFFormat[] OUTPUT_FORMATS = 
                                         new RDFFormat[] { RDFFormat.N_TRIPLES, 
                                                           RDFFormat.NOTATION_3, 
                                                           RDFFormat.RDF_XML, 
                                                           RDFFormat.TURTLE,
                                                           RDFFormat.COUNT };

    private Map<String, String> m_aliases = new HashMap<String, String>();
    
    private String m_callback = null;

    /**
     * Return true if there are any more triples.
     */
    public abstract boolean hasNext() throws TrippiException;
    
    /**
     * Return the next triple.
     */
    public abstract Triple next() throws TrippiException;

    /**
     * Release resources held by this iterator.
     */
    public abstract void close() throws TrippiException;

    public void setAliasMap(Map<String, String> aliases) {
        m_aliases = aliases;
    }

    /**
     * Gets a copy of the alias map used by this iterator.
     */
    public Map<String, String> getAliasMap() {
        return new HashMap<String, String>(m_aliases);
    }

    /**
     * Get the number of triples in the iterator, then close it.
     */
    public int count() throws TrippiException {
        try {
            int n = 0;
            while (hasNext()) {
                next();
                n++;
            }
            return n;
        } finally {
            close();
        }
    }

    /**
     * Serialize to the given stream.
     *
     * If the format is XML-based, the XML declaration will be included
     * in the output.
     *
     * After successfully writing, the TripleIterator will be closed,
     * but not the outputstream.
     */
    public int toStream(OutputStream out,
                        RDFFormat format) throws TrippiException {
        return toStream(out, format, true);
    }

    /**
     * Serialize to the given stream.
     *
     * If the format is XML-based, the XML declaration will be included
     * if <code>includeXMLDeclaration</code> is <code>true</code>.
     *
     * After successfully writing, the TripleIterator will be closed,
     * but not the outputstream.
     */
    public int toStream(OutputStream out,
                        RDFFormat format,
                        boolean includeXMLDeclaration)
            throws TrippiException {
        TripleWriter writer;
        if (format == RDFFormat.TURTLE) {
            writer = new RIOTripleWriter(new TurtleWriter(out), m_aliases);
        } else if (format == RDFFormat.RDF_XML) {
            OutputStream sink;
            if (includeXMLDeclaration) {
                sink = out;
            } else {
                sink = new XMLDeclarationRemover(out);
            }
            writer = new RIOTripleWriter(new RDFXMLWriter(sink), m_aliases);
        } else if (format == RDFFormat.N_TRIPLES) {
            writer = new RIOTripleWriter(new NTriplesWriter(out), m_aliases);
        } else if (format == RDFFormat.NOTATION_3) {
            writer = new RIOTripleWriter(new N3Writer(out), m_aliases);
        } else if (format == RDFFormat.COUNT) {
            writer = new CountTripleWriter(out);
        } else if (format == RDFFormat.COUNT_JSON) {
            writer = new FormatCountTripleWriter(new JSONTripleWriter(out, m_aliases)); 
        } else if (format == RDFFormat.JSON) {
            writer = new JSONTripleWriter(out, m_aliases); 
        } else {
            throw new TrippiException("Unsupported output format: " + format.getName());
        }
        return writer.write(this);
    }

    /**
     * Get an iterator over the triples in the given stream.
     *
     * The baseURI is used to resolve any relative URI references.
     * If given as null, http://localhost/ will be used.
     */
    public static TripleIterator fromStream(InputStream in,
                                            String baseURI,
                                            RDFFormat format) throws TrippiException {
        if (baseURI == null) baseURI = "http://localhost/";
        org.openrdf.rio.RDFParser parser;
        if (format == RDFFormat.RDF_XML) {
            parser = new org.openrdf.rio.rdfxml.RDFXMLParser();
        } else if (format == RDFFormat.TURTLE) {
            parser = new org.openrdf.rio.turtle.TurtleParser();
        } else if (format == RDFFormat.N_TRIPLES) {
            parser = new org.openrdf.rio.ntriples.NTriplesParser();
        } else {
            throw new TrippiException("Unsupported input format: " + format.getName());
        }
        return new RIOTripleIterator(in, parser, baseURI);
    }

    /**
     * Get an iterator over the triples in the given stream.
     */
    public static TripleIterator fromStream(InputStream in,
                                            RDFFormat format) throws TrippiException {
        return fromStream(in, null, format);
    }

    /**
     * Add all triples in the iterator to the given <code>Graph</code>,
     * then close the iterator.
     */
    public void addToGraph(Graph graph) throws TrippiException {

        addOrDelete(graph, true);
    }

    /**
     * Delete all triples in the iterator from the given <code>Graph</code>,
     * then close the iterator.
     */
    public void deleteFromGraph(Graph graph) throws TrippiException {

        addOrDelete(graph, false);
    }

    private void addOrDelete(Graph graph, boolean add) throws TrippiException {

        try {

            GraphElementFactory factory = graph.getElementFactory();
            TIntObjectHashMap<Node> nodePool = new TIntObjectHashMap<Node>();

            while (hasNext()) {

                Triple ot = next();

                Node subject = localize(ot.getSubject(), factory, nodePool);
                Node predicate = localize(ot.getPredicate(), factory, nodePool);
                Node object = localize(ot.getObject(), factory, nodePool);
                Triple triple = factory.createTriple((SubjectNode) subject, 
                                                     (PredicateNode) predicate, 
                                                     (ObjectNode) object);

                if (add) {
                    graph.add(triple);
                } else {
                    graph.remove(triple);
                }
            }
        } catch (GraphException e) {
            throw new TrippiException("Unable to modify Graph using TripleIterator", e);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException("Unable to modify Graph using TripleIterator", e);
        } finally {
            close();
        }
    }

    private static Node localize(Node node,
                                 GraphElementFactory factory,
                                 TIntObjectHashMap<Node> nodePool) throws TrippiException {

        try {
            if (nodePool.containsKey(node.hashCode())) {
                return nodePool.get(node.hashCode());
            } else {
                Node newNode;
                if (node instanceof URIReference) {
                    newNode = factory.createResource(((URIReference) node).getURI(), false);
                } else if (node instanceof Literal) {
                    Literal literal = (Literal) node;
                    if (literal.getLanguage() != null) {
                        newNode = factory.createLiteral(literal.getLexicalForm(), 
                                                        literal.getLanguage());
                    } else if (literal.getDatatypeURI() != null) {
                        newNode = factory.createLiteral(literal.getLexicalForm(), 
                                                        literal.getDatatypeURI());
                    } else {
                        newNode = factory.createLiteral(literal.getLexicalForm());
                    }
                } else if (node instanceof BlankNode) {
                    newNode = factory.createResource();
                } else {
                    throw new TrippiException("Unrecognized node type: " + node.getClass().getName());
                }
                nodePool.put(node.hashCode(), newNode);
                return newNode;
            }
        } catch (Throwable th) {
            throw new TrippiException("Unable to localize node", th);
        }
    }
    
	public void setCallback(String callback) {
		if (callback != null) m_callback  = callback;
	}
	
	public String getCallback(){
		return m_callback;
	}

}
