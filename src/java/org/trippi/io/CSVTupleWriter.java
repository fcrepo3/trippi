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
 * Writes tuples as CSV's (comma-separated values), a format common in
 * spreadsheet applications.
 *
 * Headings (the names in the first row) are always quoted.
 * Values are quoted only when they contain commas or quote chars (in which 
 * case the quote character is escaped using "").  Newlines in values
 * are replaced with spaces.
 *
 * Note that this form does not preserve datatype or language attributes
 * for RDF literals.  UTF-8 encoding is used for extended characters.
 */
public class CSVTupleWriter extends TupleWriter {

    private PrintWriter m_out;
    private Map<String, String> m_aliases;

    public CSVTupleWriter(OutputStream out, Map<String, String> aliases) throws TrippiException {
        try {
            m_out = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
            m_aliases = aliases;
        } catch (IOException e) {
            throw new TrippiException("Error setting up writer", e);
        }
    }

    public int write(TupleIterator iter) throws TrippiException {
        try {
            String[] names = iter.names();
            for (int i = 0; i < names.length; i++) {
                if (i > 0) m_out.print(",");
                addQuoted(names[i]);
            }
            m_out.println();
            int count = 0;
            while (iter.hasNext()) {
                Map<String, Node> result = iter.next();
                for (int i = 0; i < names.length; i++) {
                    if (i > 0) m_out.print(",");
                    String val = getValue(result.get(names[i]));
                    if (val.indexOf(",") == -1 && val.indexOf("\"") == -1) {
                        m_out.print(val);
                    } else {
                        addQuoted(val);
                    }
                }
                m_out.println();
                m_out.flush();
                count++;
            }
            m_out.flush();
            iter.close();
            return count;
        } catch (IOException e) {
            throw new TrippiException("Error writing", e);
        }
    }

    private void addQuoted(String val) throws IOException {
        m_out.print("\"");
        m_out.print(val.replaceAll("\"", "\"\""));
        m_out.print("\"");
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
