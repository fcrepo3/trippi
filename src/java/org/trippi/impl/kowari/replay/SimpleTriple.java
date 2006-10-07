package org.trippi.impl.kowari.replay;

import org.jrdf.graph.*;

public class SimpleTriple extends AbstractTriple {

    public SimpleTriple(SubjectNode subjectNode,
                        PredicateNode predicateNode,
                        ObjectNode objectNode) {
        this.subjectNode = subjectNode;
        this.predicateNode = predicateNode;
        this.objectNode = objectNode;
    }

    public String toTripleString() {
        return toString(this);
    }

    public static String toString(Node node) {
        if (node == null) return "null";
        if (node instanceof URIReference) {
            URIReference n = (URIReference) node;
            return "<" + n.getURI().toString() + ">";
        } else if (node instanceof BlankNode) {
            return "_node" + node.hashCode();
        } else {
            Literal n = (Literal) node;
            StringBuffer out = new StringBuffer();
            out.append("\"" + escapeLiteral(n.getLexicalForm()) + "\"");
            if (n.getLanguage() != null && n.getLanguage().length() > 0) {
                out.append("@" + n.getLanguage());
            } else if (n.getDatatypeURI() != null) {
                out.append("^^" + n.getDatatypeURI().toString());
            }
            return out.toString();
        }
    }

    public static String toString(Triple triple) {
        return toString(triple.getSubject()) + " "
             + toString(triple.getPredicate()) + " "
             + toString(triple.getObject());
    }

    private static String escapeLiteral(String s) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ( c == '"' ) {
                out.append("\\\"");
            } else if ( c == '\\' ) {
                out.append("\\\\");
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

}
