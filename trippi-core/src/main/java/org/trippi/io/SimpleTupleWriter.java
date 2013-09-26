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
 * A simple, easy-to-read format for tuples.
 */
public class SimpleTupleWriter extends TupleWriter {

    private PrintWriter m_out;
    private Map<String, String> m_aliases;

    public SimpleTupleWriter(OutputStream out, Map<String, String> aliases) throws TrippiException {
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
            int longest = 0;
            for (int i = 0; i < names.length; i++) {
                if (names[i].length() > longest) longest = names[i].length();
            }
            int count = 0;
            while (iter.hasNext()) {
                Map<String, Node> result = iter.next();
                for (int i = 0; i < names.length; i++) {
                    m_out.print(names[i]);
                    SpaceCharacters.indent(longest - names[i].length(), m_out);
                    m_out.print(" : ");
                    m_out.println(getString(result.get(names[i])));
                }
                m_out.println("");
                m_out.flush();
                count++;
            }
            m_out.flush();
            iter.close();
            return count;
    }

    public String getString(Node node) {
        String fullString = RDFUtil.toString(node);
        if (m_aliases != null) {
            if (node instanceof URIReference) {
                Iterator<String> iter = m_aliases.keySet().iterator();
                while (iter.hasNext()) {
                    String alias = iter.next();
                    String prefix = m_aliases.get(alias);
                    if (fullString.startsWith("<" + prefix)) {
                        return "<" + alias + ":" + fullString.substring(prefix.length() + 1);
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
                            return out.toString();
                        }
                    }
                }
            }
        }
        return fullString;
    }

    @Deprecated
    public static String indent(int num) {
        return new String(SpaceCharacters.indentChars(num));
    }

}
