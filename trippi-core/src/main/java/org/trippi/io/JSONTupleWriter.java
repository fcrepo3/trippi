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
import org.trippi.Alias;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.impl.base.AliasManager;

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
public class JSONTupleWriter extends TupleWriter {

    private PrintWriter m_out;
    private AliasManager m_aliases;
    public JSONTupleWriter(OutputStream out, Map<String, String> aliases) throws TrippiException {
        try {
            m_out = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
            m_aliases = new AliasManager(aliases);
        } catch (IOException e) {
            throw new TrippiException("Error setting up writer", e);
        }
    }
    
    public JSONTupleWriter(OutputStream out, AliasManager aliases) throws TrippiException {
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

            m_out.println("{\"results\":[");
            int count = 0;
            while (iter.hasNext()) {
            	if (count > 0) m_out.print(',');
            	m_out.print('{');
                Map<String, Node> result = iter.next();
                for (int i = 0; i < names.length; i++) {
                    if (i > 0) m_out.print(',');
                    m_out.print('"');
                    m_out.print(names[i]);
                    m_out.print("\" : ");
                    String val = getValue(result.get(names[i]));
                    if (val.indexOf(',') == -1 && val.indexOf('"') == -1) {
                    	m_out.print('"');
                        m_out.print(val);
                    	m_out.print('"');
                    } else {
                        addQuoted(val);
                    }
                }
                m_out.println('}');
                m_out.flush();
                count++;
            }
            m_out.print("]}");
            m_out.flush();
            iter.close();
            return count;
        } catch (IOException e) {
            throw new TrippiException("Error writing", e);
        }
    }

    private void addQuoted(String val) throws IOException {
        m_out.print('"');
        m_out.print(val.replaceAll("\"", "\\\\\""));
        m_out.print('"');
    }

    public String getValue(Node node) {
        String fullString = RDFUtil.toString(node);
        if (m_aliases != null) {
            if (node instanceof URIReference) {
                Iterator<Alias> iter = m_aliases.getAliases().values().iterator();
                while (iter.hasNext()) {
                    Alias a = iter.next();
                    String alias = a.getKey();
                    String expansion = a.getExpansion();
                    if (fullString.startsWith("<" + expansion)) {
                        return fix("<" + alias + ":" + fullString.substring(expansion.length() + 1));
                    }
                }
            } else if (node instanceof Literal) {
                Literal literal = (Literal) node;
                if (literal.getDatatypeURI() != null) {
                    String uri = literal.getDatatypeURI().toString();
                    Iterator<Alias> iter = m_aliases.getAliases().values().iterator();
                    while (iter.hasNext()) {
                        Alias a = iter.next();
                        String alias = a.getKey();
                        String expansion = a.getExpansion();
                        if (uri.startsWith(expansion)) {
                            StringBuffer out = new StringBuffer();
                            out.append('"');
                            out.append(literal.getLexicalForm().replaceAll("\"", "\\\""));
                            out.append("\"^^");
                            out.append(alias);
                            out.append(':');
                            out.append(uri.substring(expansion.length()));
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
