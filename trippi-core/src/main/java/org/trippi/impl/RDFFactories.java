package org.trippi.impl;

import java.net.URI;
import java.net.URISyntaxException;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;


public abstract class RDFFactories {
    
    public static final GraphElementFactory FACTORY =
            new StatelessGraphElementFactory();
    /**
     * Create a BlankNode given a unique id.
     */
    public static BlankNode createResource(int hashCode) {
        return new FreeBlankNode(hashCode);
    }

    /////// org.jrdf.graph.GraphElementFactory ///////

    public static Literal createLiteral(String lexicalValue) 
            throws GraphElementFactoryException {
        return new FreeLiteral(lexicalValue);
    }

    public static Literal createLiteral(String lexicalValue, String languageType) 
            throws GraphElementFactoryException {
        return new FreeLiteral(lexicalValue, languageType);
    }

    public static Literal createLiteral(String lexicalValue, URI datatypeURI) 
            throws GraphElementFactoryException {
        return new FreeLiteral(lexicalValue, datatypeURI);
    }

    public static BlankNode createResource()
            throws GraphElementFactoryException {
        return new FreeBlankNode(new Object());
    }

    public static URIReference createResource(URI uri) 
            throws GraphElementFactoryException {
        return new FreeURIReference(uri);
    }

    public static URIReference createResource(URI uri, boolean validate) 
            throws GraphElementFactoryException {
        return new FreeURIReference(uri, validate);
    }

    public static Triple createTriple(SubjectNode subject, 
                               PredicateNode predicate, 
                               ObjectNode object) 
            throws GraphElementFactoryException {
        return new FreeTriple(subject, predicate, object);
    }

    // parsing convenience methods
    
    public static Triple createTriple(
            org.openrdf.model.Resource subject,
            org.openrdf.model.URI predicate,
            org.openrdf.model.Value object) 
                    throws GraphElementFactoryException,
                    URISyntaxException {
        return createTriple( subjectNode(subject),
                predicateNode(predicate),
                objectNode(object));
    }

    public static SubjectNode subjectNode(org.openrdf.model.Resource subject) 
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (subject instanceof org.openrdf.model.URI) {
            return createResource( new URI(((org.openrdf.model.URI) subject).stringValue()) );
        } else {
            return createResource(((org.openrdf.model.BNode) subject).getID().hashCode());
        }
    }

    public static PredicateNode predicateNode(org.openrdf.model.URI predicate)
            throws GraphElementFactoryException,
                   URISyntaxException {
        return createResource( new URI((predicate).stringValue()) );
    }

    public static ObjectNode objectNode(org.openrdf.model.Value object)
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (object instanceof org.openrdf.model.URI) {
            return createResource( new URI(((org.openrdf.model.URI) object).stringValue()) );
        } else if (object instanceof  org.openrdf.model.Literal) {
            org.openrdf.model.Literal lit = (org.openrdf.model.Literal) object;
            org.openrdf.model.URI uri = lit.getDatatype();
            String lang = lit.getLanguage();
            if (uri != null) {
                // typed 
                return createLiteral(lit.getLabel(), new URI(uri.toString()));
            } else if (lang != null && !lang.equals("")) {
                // local
                return createLiteral(lit.getLabel(), lang);
            } else {
                // plain
                return createLiteral(lit.getLabel());
            }
        } else {
            return createResource(((org.openrdf.model.BNode) object).getID().hashCode());
        }
    }
    
    private static class StatelessGraphElementFactory implements GraphElementFactory {
        @Override
        public Literal createLiteral(String lexicalValue) 
                throws GraphElementFactoryException {
            return RDFFactories.createLiteral(lexicalValue);
        }

        @Override
        public Literal createLiteral(String lexicalValue, String languageType) 
                throws GraphElementFactoryException {
            return RDFFactories.createLiteral(lexicalValue, languageType);
        }

        @Override
        public Literal createLiteral(String lexicalValue, URI datatypeURI) 
                throws GraphElementFactoryException {
            return RDFFactories.createLiteral(lexicalValue, datatypeURI);
        }

        @Override
        public BlankNode createResource()
                throws GraphElementFactoryException {
            return RDFFactories.createResource();
        }

        @Override
        public URIReference createResource(URI uri) 
                throws GraphElementFactoryException {
            return RDFFactories.createResource(uri);
        }

        @Override
        public URIReference createResource(URI uri, boolean validate) 
                throws GraphElementFactoryException {
            return RDFFactories.createResource(uri, validate);
        }

        @Override
        public Triple createTriple(SubjectNode subject, 
                                   PredicateNode predicate, 
                                   ObjectNode object) 
                throws GraphElementFactoryException {
            return RDFFactories.createTriple(subject, predicate, object);
        }
    }
}
