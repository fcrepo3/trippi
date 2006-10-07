package org.trippi;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;

/**
 * A pattern that can be used to derive a Triple from a Map of Node objects.
 */
public class TriplePattern {

    private Object m_subject;
    private Object m_predicate;
    private Object m_object;

    public TriplePattern(Object subject,
                         Object predicate,
                         Object object) throws TrippiException {
        if ( !(subject instanceof String) && 
                !(subject instanceof SubjectNode) ) {
            throw new TrippiException("Error in triple pattern: subject "
                    + "must be a String or a SubjectNode (not " 
                    + subject.getClass().getName() + ")");
        }
        if ( !(predicate instanceof String) &&
                !(predicate instanceof PredicateNode) ) {
            throw new TrippiException("Error in triple pattern: predicate "
                    + "must be a String or a PredicateNode (not " 
                    + predicate.getClass().getName() + ")");
        }
        if ( !(object instanceof String) &&
                !(object instanceof ObjectNode) ) {
            throw new TrippiException("Error in triple pattern: object "
                    + "must be a String or an ObjectNode (not " 
                    + object.getClass().getName() + ")");
        }
        m_subject = subject;
        m_predicate = predicate;
        m_object = object;
    }

    public static TriplePattern[] parse(String patterns) 
                                         throws TrippiException {
        try {
            List tokens = tokenize(patterns.replaceAll("\r", " "));
            if (tokens.size() % 3 != 0 || tokens.size() == 0) {
                throw new TrippiException("Triple pattern token count not divisible by 3.");
            }
            TriplePattern[] out = new TriplePattern[tokens.size() / 3];
            Iterator iter = tokens.iterator();
            int i = 0;
            RDFUtil factory = new RDFUtil();
            while (iter.hasNext()) {
                Object subject = parseToken((String) iter.next(), factory);
                Object predicate = parseToken((String) iter.next(), factory);
                Object object = parseToken((String) iter.next(), factory);
                out[i++] = new TriplePattern(subject, predicate, object);
            }
            return out;
        } catch (Exception e) {
            String msg = "Parse error: " + e.getClass().getName();
            if (e.getMessage() != null) msg += ": " + e.getMessage();
            throw new TrippiException(msg, e);
        }
    }

    /**
     * Parse and return a Set of Triples, treating variable bindings
     * as anonymous nodes.
     */
    public static List parse(String triples,
                             GraphElementFactory factory) throws TrippiException {
        try {
        TriplePattern[] patterns = parse(triples);
        HashMap bNodes = new HashMap();
        ArrayList list = new ArrayList();
        for (int i = 0; i < patterns.length; i++) {
            Object s = patterns[i].getSubject();
            Object p = patterns[i].getPredicate();
            Object o = patterns[i].getObject();
            SubjectNode subject;
            PredicateNode predicate;
            ObjectNode object;
            if (s instanceof String) {
                BlankNode b = (BlankNode) bNodes.get(s);
                if (b == null) {
                    b = factory.createResource();
                    bNodes.put(s, b);
                }
                subject = b;
            } else {
                subject = (SubjectNode) s;
            }
            if (p instanceof String) {
                throw new TrippiException("Predicate cannot be bNode.");
            } else {
                predicate = (PredicateNode) p;
            }
            if (o instanceof String) {
                BlankNode b = (BlankNode) bNodes.get(o);
                if (b == null) {
                    b = factory.createResource();
                    bNodes.put(o, b);
                }
                object = b;
            } else {
                object = (ObjectNode) o;
            }
            list.add(factory.createTriple(subject, predicate, object));
        }
        return list;
        } catch (Exception e) {
            throw new TrippiException("Error creating triples from string.", e);
        }
    }

    private static Object parseToken(String token,
                                    RDFUtil factory) 
                throws GraphElementFactoryException,
                       URISyntaxException {
        char c = token.charAt(0);
        if ( c == '\'' || c == '"' || c == '<' || c == '_' ) {
            return factory.parse(token);
        } else if ( c == '$' || c == '?' ) {
            return token.substring(1);
        } else {
            return token;
        }
    }

    private static List tokenize(String patterns) {
        List tokens = new ArrayList();
        boolean inToken = false;
        boolean inLiteral = false;
        boolean lastWasEscape = false;
        boolean inQuotedPart = false;
        char qChar=' ';
        StringBuffer token = null;
        for (int i = 0; i < patterns.length(); i++) {
            char c = patterns.charAt(i);
            if (inToken) {
                if (inLiteral) {
                    if (inQuotedPart) {
                        if (lastWasEscape) {
                            lastWasEscape = false;
                            token.append(c);
                        } else {
                            if (c == qChar) {
                                token.append(c);
                                inQuotedPart = false;
                                if ( i + 1 < patterns.length() ) {
                                    // are we out of the literal?
                                    char n = patterns.charAt(i + 1);
                                    if (n == '\n' || n == '\t' || n == ' ' || n == ',' || n == '(' || n == ')') {
                                        inToken = false;
                                        inLiteral = false;
                                        tokens.add(token.toString());
                                    }
                                }
                            } else {
                                if (c == '\\') {
                                    lastWasEscape = true;
                                }
                                token.append(c);
                            }
                        }
                    } else {
                        if (c == '\n' || c == '\t' || c == ' ' || c == ',' || c == '(' || c == ')') {
                            inToken = false;
                            inLiteral = false;
                            tokens.add(token.toString());
                        } else {
                            token.append(c);
                        }
                    }
                } else {
                    if (c == '\n' || c == '\t' || c == ' ' || c == ',' || c == '(' || c == ')') {
                        inToken = false;
                        tokens.add(token.toString());
                    } else {
                        token.append(c);
                    }
                }
            } else {
                if (c != '\n' && c != '\t' && c != ' ' && c != ',' && c != '(' && c != ')') {
                    inToken = true;
                    if (c == '\"' || c == '\'') {
                        inLiteral = true;
                        inQuotedPart = true;
                        lastWasEscape = false;
                        qChar = c;
                    }
                    token = new StringBuffer();
                    token.append(c);
                }
            }
            if (inToken == false) token = null;
        }
        if (token != null) {
            tokens.add(token.toString());
        }
        return tokens;
    }

    /**
     * Get the subject of the pattern.
     *
     * This will either be a String (a binding name) or a JRDF SubjectNode.
     *
     * @see org.jrdf.graph.SubjectNode
     */
    public Object getSubject() {
        return m_subject;
    }

    /**
     * Get the predicate of the pattern.
     *
     * This will either be a String (a binding name) or a JRDF PredicateNode.
     *
     * @see org.jrdf.graph.PredicateNode
     */
    public Object getPredicate() {
        return m_predicate;
    }

    /**
     * Get the object of the pattern.
     *
     * This will either be a String (a binding name) or a JRDF ObjectNode.
     *
     * @see org.jrdf.graph.ObjectNode
     */
    public Object getObject() {
        return m_object;
    }

    /**
     * Get a Triple that matches this pattern using the provided tuple.
     *
     * @return Triple if a Triple can be constructed, or null if any of the
     *                bound variables in the map are null or wouldn't
     *                match the required type for the triple.
     * @throws TrippiException if the Map does not contain a key that
     *                              matches a binding name in this pattern.
     */
    public Triple match(Map tuple) throws TrippiException {
        SubjectNode subject;
        PredicateNode predicate;
        ObjectNode object;

        try {
            RDFUtil factory = new RDFUtil();

            if (m_subject instanceof String) {
                Node n = getMatchingNode((String) m_subject, tuple, 0);
                if (n == null) return null;
                subject = (SubjectNode) n;
            } else {
                if (m_subject instanceof BlankNode) {
                    // Get a bNode that is unique to the tuple, but may be
                    // the same as that for other TriplePatterns being evaluated 
                    // against the same map
                    subject = factory.createResource(
                               tuple.hashCode() + m_subject.hashCode());
                } else {
                    subject = (SubjectNode) m_subject;
                }
            }

            if (m_predicate instanceof String) {
                Node n = getMatchingNode((String) m_predicate, tuple, 1);
                if (n == null) return null;
                predicate = (PredicateNode) n;
            } else {
                predicate = (PredicateNode) m_predicate;
            }

            if (m_object instanceof String) {
                Node n = getMatchingNode((String) m_object, tuple, 2);
                if (n == null) return null;
                object = (ObjectNode) n;
            } else {
                if (m_object instanceof BlankNode) {
                    // Get a bNode that is unique to the tuple, but may be
                    // the same as that for other TriplePatterns being evaluated 
                    // against the same map
                    object = factory.createResource(
                                 tuple.hashCode() + m_object.hashCode());
                } else {
                    object = (ObjectNode) m_object;
                }
            }

            return factory.createTriple(subject, predicate, object);

        } catch (Exception e) {
            String msg = e.getClass().getName();
            if (e.getMessage() != null) msg += ": " + e.getMessage();
            throw new TrippiException(msg, e);
        }


    }

    // TODO: avoid RE_USING BNODES???  DISTINCT BNODE PER RESULT BINDING!!!
    // 
    // USE MAP HASHKEY....?

    private Node getMatchingNode(String key, 
                                 Map tuple,
                                 int type) throws TrippiException {
        if (tuple.containsKey(key)) {
            Node node = (Node) tuple.get(key);
            if (node == null) return null; // null value bound for this tuple
            if (type == 0) {
                if (node instanceof SubjectNode) return node;
                return null;
            } else if (type == 1) {
                if (node instanceof PredicateNode) return node;
                return null;
            } else {
                if (node instanceof ObjectNode) return node;
                return null;
            }
        } else {
            throw new TrippiException("No value named '" + key + "' in tuple.");
        }
    }

    public String toString() {
        StringBuffer out = new StringBuffer();
        if (m_subject instanceof String) {
            out.append(m_subject + " ");
        } else {
            out.append(RDFUtil.toString((Node) m_subject) + " ");
        }
        if (m_predicate instanceof String) {
            out.append(m_predicate + " ");
        } else {
            out.append(RDFUtil.toString((Node) m_predicate) + " ");
        }
        if (m_object instanceof String) {
            out.append(m_object);
        } else {
            out.append(RDFUtil.toString((Node) m_object));
        }
        return out.toString();
    }

    public static void main(String[] args) throws Exception {
        System.out.print("Patterns: ");
        TriplePattern[] patterns = TriplePattern.parse(
            new java.io.BufferedReader(
                new java.io.InputStreamReader(System.in)
            ).readLine()
        );
        for (int i = 0; i < patterns.length; i++) {
            System.out.println("Pattern " + i + ":" + patterns[i].toString());
        }
    }

}
