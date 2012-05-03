package org.trippi.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.URIReference;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class SparqlW3CTupleWriter extends TupleWriter {

    private PrintWriter m_out;
    private Map<String, String> m_aliases;

    public SparqlW3CTupleWriter(OutputStream out, Map<String, String> aliases) throws TrippiException {
        try {
            m_out = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
            m_aliases = aliases;
        } catch (IOException e) {
            throw new TrippiException("Error setting up writer", e);
        }
    }

    @Override
	public int write(TupleIterator iter) throws TrippiException {
        try {
            m_out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            doEntities();
            m_out.println("<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">");
            String[] names = iter.names();
            m_out.println("  <head>");
            for (int i = 0; i < names.length; i++) {
                m_out.println("    <variable name=\"" + names[i] + "\"/>");
            }
            m_out.println("  </head>");
            m_out.println("  <results>");
            int count = 0;
            while (iter.hasNext()) {
                m_out.println("    <result>");
                Map<String, Node> result = iter.next();
                for (int i = 0; i < names.length; i++) {
                    m_out.print("      <" + names[i]);
                    Node n = result.get(names[i]);
                    if ( n == null ) {
                        m_out.println(" bound=\"false\"/>");
                    } else if ( n instanceof URIReference ) {
                        String uriString = ((URIReference) n).getURI().toString();
                        m_out.println(" uri=\"" + getURI(uriString) + "\"/>");
                    } else if ( n instanceof BlankNode ) {
                        String id = "blank" + n.hashCode();
                        m_out.println(" bnodeid=\"" + id + "\"/>");
                    } else if ( n instanceof Literal ) {
                        Literal lit = (Literal) n;
                        URI dType = lit.getDatatypeURI();
                        if (dType != null) {
                            m_out.print(" datatype=\"" + getURI(dType.toString()) + "\"");
                        }
                        String lang = lit.getLanguage();
                        if (lang != null) {
                            m_out.print(" xml:lang=\"" + lang + "\"");
                        }
                        m_out.println(">" + enc(lit.getLexicalForm()) + "</" + names[i] + ">");
                    } else {
                        throw new TrippiException("Unrecognized node type: " + n.getClass().getName());
                    }
                }
                m_out.println("    </result>");
                m_out.flush();
                count++;
            }
            m_out.println("  </results>");
            m_out.println("</sparql>");
            m_out.flush();
            iter.close();
            return count;
        } catch (IOException e) {
            throw new TrippiException("Error writing", e);
        }
    }

    private void doEntities() throws IOException {
        if (m_aliases == null || m_aliases.keySet().size() == 0) return;
        m_out.println("<!DOCTYPE sparql [");
        Iterator<String> iter = m_aliases.keySet().iterator();
        while (iter.hasNext()) {
            String ent = iter.next();
            String value = m_aliases.get(ent);
            m_out.println("  <!ENTITY " + ent + " \"" + value + "\">");
        }
        m_out.println("]>");
    }

    private String getURI(String s) {
        if (m_aliases == null || m_aliases.keySet().size() == 0) return enc(s);
        Iterator<String> iter = m_aliases.keySet().iterator();
        while (iter.hasNext()) {
            String alias = iter.next();
            String prefix = m_aliases.get(alias);
            if (s.startsWith(prefix)) {
                return "&" + alias + ";" + enc(s.substring(prefix.length()));
            }
        }
        return enc(s);
    }

}
