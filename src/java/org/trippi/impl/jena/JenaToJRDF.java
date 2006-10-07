package org.trippi.impl.jena;

import java.net.URI;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.URIReference;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;

public class JenaToJRDF {

    private RDFUtil m_factory;

    public JenaToJRDF() throws TrippiException {
        try {
            m_factory = new RDFUtil();
        } catch (Exception e) {
            String msg = "Error creating RDFUtil: " 
                    + e.getClass().getName();
            if (e.getMessage() != null) msg += ": " + e.getMessage();
            throw new TrippiException(msg, e);
        }
    }

    public Node toNode(com.hp.hpl.jena.graph.Node jenaNode) 
            throws TrippiException {
        if (jenaNode == null) return null;
        Node jrdfNode = null;
        try {
            if (jenaNode.isURI()) {
                jrdfNode = toURIReference((com.hp.hpl.jena.graph.Node_URI) jenaNode);
            } else if (jenaNode.isLiteral()) {
                jrdfNode = toLiteral((com.hp.hpl.jena.graph.Node_Literal) jenaNode);
            } else if (jenaNode.isBlank()) {
                jrdfNode = toBlankNode((com.hp.hpl.jena.graph.Node_Blank) jenaNode);
            }
        } catch (Exception e) {
            String msg = "Error converting Jena Node to JRDF Node: "
                    + e.getClass().getName();
            if (e.getMessage() != null) msg += ": " + e.getMessage();
            throw new TrippiException(msg, e);
        } finally {
            if ( jrdfNode == null ) {
                throw new TrippiException("Unrecognized Jena Node type: "
                        + jenaNode.getClass().getName());
            }
        }
        return jrdfNode;
    }

    public URIReference toURIReference(
            com.hp.hpl.jena.graph.Node_URI jenaURI) throws Exception {
        return m_factory.createResource( new URI(jenaURI.getURI()) );
    }

    public Literal toLiteral(
            com.hp.hpl.jena.graph.Node_Literal jenaLiteral) throws Exception {
        String value = jenaLiteral.getLiteral().getLexicalForm();
        String datatype = jenaLiteral.getLiteral().getDatatypeURI();
        if ( datatype != null ) {
            return m_factory.createLiteral(value, new URI(datatype));
        } else {
            String lang = jenaLiteral.getLiteral().language();
            if (lang == null || lang.equals("")) {
                return m_factory.createLiteral(value);
            } else {
                return m_factory.createLiteral(value, lang);
            }
        }
    }

    public BlankNode toBlankNode(
            com.hp.hpl.jena.graph.Node_Blank jenaBlank) throws Exception {
        return m_factory.createResource(jenaBlank.getBlankNodeId().hashCode());
    }    

}