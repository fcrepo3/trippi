package org.trippi.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.URIReference;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * Writes tuples as TSV's (tab-separated values), a format common in
 * spreadsheet applications.
 *
 * No escaping or quoting is used, but tabs and newlines within values
 * are converted to spaces.
 *
 * Note that this form does not preserve datatype or language attributes
 * for RDF literals.  UTF-8 encoding is used for extended characters.
 */
public class TSVTupleWriter extends TupleWriter {

    private PrintWriter m_out;
    private Map<String, String> m_aliases;

    public TSVTupleWriter(OutputStream out, Map<String, String> aliases) throws TrippiException {
        try {
            m_out = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
            m_aliases = aliases;
        } catch (IOException e) {
            throw new TrippiException("Error setting up writer", e);
        }
    }

    @Override
	public int write(TupleIterator iter) throws TrippiException {
        String[] names = iter.names();
        for (int i = 0; i < names.length; i++) {
            if (i > 0) m_out.print("\t");
            m_out.print(names[i]);
        }
        m_out.println();
        int count = 0;
        while (iter.hasNext()) {
            Map<String, Node> result = iter.next();
            for (int i = 0; i < names.length; i++) {
                if (i > 0) m_out.print("\t");
                String val = getValue(result.get(names[i]));
                m_out.print(val.replaceAll("\t", " ").replaceAll("\n", " "));
            }
            m_out.println();
            m_out.flush();
            count++;
        }
        m_out.flush();
        iter.close();
        return count;
    }

    public String getValue(Node node) {
        String fullString = RDFUtil.toString(node);
        if (m_aliases != null) {
            if (node instanceof URIReference) {
                Iterator<String> iter = m_aliases.keySet().iterator();
                while (iter.hasNext()) {
                    String alias = iter.next();
                    String prefix = m_aliases.get(alias);
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
            return lit.replaceAll("\\\\\"", "\"");
        } else if (in.startsWith("<")) {
            // resource
            return in.substring(1, in.length() - 1);
        } else {
            return in;
        }
    }

}
