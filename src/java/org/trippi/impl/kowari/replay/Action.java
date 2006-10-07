package org.trippi.impl.kowari.replay;

import java.util.*;

import org.jrdf.graph.*;

public class Action {

    public static final int ADD = 0;
    public static final int DEL = 1;
    public static final int SPO = 2;
    public static final int ITQ = 3;

    private Date m_date;
    private int m_type;

    /** If type is ADD or DEL, this holds the parsed triples. */
    private List m_triples;

    /** If type is ITQ, this holds the query text. */
    private String m_query;

    /** If type is SPO, this holds the query as a Triple. */
    private Triple m_pattern;

    /**
     * Constructor for add or delete action.
     */
    public Action(Date date, int type, List triples) throws Exception {
        m_date = date;
        m_type = type;
        m_triples = triples;
    }

    /**
     * Constructor for pre-parsed spo query action.
     */
    public Action(Date date, Triple pattern) throws Exception {
        m_date = date;
        m_type = SPO;
        m_pattern = pattern;
    }

    /**
     * Constructor for other query action (such as itq).
     */
    public Action(Date date, int type, String query) throws Exception {
        m_date = date;
        m_type = type;
        m_query = query;
    }

    public static int getType(String actionType) throws Exception {
        if (actionType.equals("add")) return ADD;
        if (actionType.equals("del")) return DEL;
        if (actionType.equals("spo")) return SPO;
        if (actionType.equals("itq")) return ITQ;
        throw new Exception("Unrecognized action type: " + actionType);
    }

    public Date getDate() { return m_date; }
    public int getType() { return m_type; }

    // ADD or DEL
    public List getTriples() { return m_triples; }

    // ITQ
    public String getQuery() { return m_query; }

    // SPO
    public Triple getPattern() { return m_pattern; }

    public String toShortString() {
        if (m_type == SPO) return "s";
        if (m_type == ITQ) return "i";
        if (m_type == ADD) return "a" + m_triples.size();
        if (m_type == DEL) return "d" + m_triples.size();
        return "UNKNOWN-ACTION";
    }
}