package org.trippi;

/**
 * Data formats RDF triples/tuples.
 *
 * @author cwilper@cs.cornell.edu
 */
public class RDFFormat {

    /** 
     * An older subset of Notation 3 defined in the
     * <a href="http://www.w3.org/TR/rdf-testcases/#ntriples">RDF Test Cases</a>
     * document.
     */
    public static final RDFFormat N_TRIPLES = new RDFFormat("N-Triples",
                                                            "US-ASCII",
                                                            "text/plain",
                                                            ".nt");

    /** 
     * The original RDF text format, defined by TimBL in 
     * <a href="http://www.w3.org/DesignIssues/Notation3.html">An RDF language 
     * for the Semantic Web</a>.
     */
    public static final RDFFormat NOTATION_3 = new RDFFormat("Notation 3",
                                                             "UTF-8",
                                                             "text/rdf+n3",
                                                             ".n3");

    /**
     * The "RDF/XML" format, defined in the
     * <a href="http://www.w3.org/TR/rdf-syntax-grammar/">RDF/XML 
     * Syntax Specification</a>
     */
    public static final RDFFormat RDF_XML = new RDFFormat("RDF/XML",
                                                          "UTF-8",
                                                          "application/rdf+xml",
                                                          ".xml");

    /**
     * A newer subset of Notation 3, defined in Dave Beckett's 
     * <a href="http://www.ilrt.bris.ac.uk/discovery/2004/01/turtle/">Turtle -
     * Terse RDF Triple Language</a>.
     */
    public static final RDFFormat TURTLE = new RDFFormat("Turtle",
                                                         "UTF-8",
                                                         "application/x-turtle",
                                                         ".ttl");

    /**
     * Comma-separated values.
     */
    public static final RDFFormat CSV = new RDFFormat("CSV",
                                                      "UTF-8",
                                                      "text/plain",
                                                      ".csv");

    /**
     * The format devised by the 
     * <a href="http://www.w3.org/2001/sw/DataAccess/">RDF Data Access Working 
     * Group</a> to return SPARQL query results.
     *
     * This format is <a href="http://www.w3.org/TR/2004/WD-rdf-sparql-XMLres-20041221/">still in development</a>.
     */
    public static final RDFFormat SPARQL = new RDFFormat("Sparql",
                                                         "UTF-8",
                                                         "application/xml",
                                                         ".xml");

    /**
     * A simple text format that has each name-value pair on a separate line.
     */
    public static final RDFFormat SIMPLE = new RDFFormat("Simple",
                                                         "UTF-8",
                                                         "text/plain",
                                                         ".txt");

    /**
     * Tab-separated values.
     */
    public static final RDFFormat TSV = new RDFFormat("TSV",
                                                      "UTF-8",
                                                      "text/plain",
                                                      ".tsv");
                                                      
    /**
     * Simple 'count' output format - plain text number only.
     */
    public static final RDFFormat COUNT = new RDFFormat("count",
                                                      "UTF-8",
                                                      "text/plain",
                                                      ".txt");
    
    /**
     * <a href="http://json.org/">JSON</a> format
     */
    public static final RDFFormat JSON = new RDFFormat("json",
            "UTF-8",
            "application/json",
            ".js");
    /**
     * Formatted counts
     */
    public static final RDFFormat COUNT_JSON = new RDFFormat("count/json",
            "UTF-8",
            "application/json",
            ".js");
    
    public static final RDFFormat COUNT_SPARQL = new RDFFormat("count/Sparql",
            "UTF-8",
            "application/xml",
            ".xml");   
    
    public static final RDFFormat SPARQL_W3C = new RDFFormat("Sparql_W3C",
            "UTF-8",
            "application/xml",
            ".xml");   
    

    public static final RDFFormat[] ALL = new RDFFormat[] { N_TRIPLES,
                                                            NOTATION_3,
                                                            RDF_XML,
                                                            TURTLE,
                                                            CSV,
                                                            SIMPLE,
                                                            SPARQL,
                                                            SPARQL_W3C,
                                                            TSV,
                                                            JSON,
                                                            COUNT,
                                                            COUNT_JSON,
                                                            COUNT_SPARQL,
                                                            COUNT };

    private String m_name;
    private String m_encoding;
    private String m_mediaType;
    private String m_extension;

    private RDFFormat(String name,
                      String encoding,
                      String mediaType,
                      String extension) {
        m_name = name;
        m_encoding = encoding;
        m_mediaType = mediaType;
        m_extension = extension;
    }

    public String getName() { return m_name; }
    public String getEncoding() { return m_encoding; }
    public String getMediaType() { return m_mediaType; }
    public String getExtension() { return m_extension; }

    public static RDFFormat forName(String name) throws TrippiException {
        String s = simplifyName(name);
        for (int i = 0; i < ALL.length; i++) {
            if (s.equals(simplifyName(ALL[i].getName()))) return ALL[i];
        }
        throw new TrippiException("Unrecognized format: " + name);
    }

    private static final String simplifyName(String name) {
        name = name.toLowerCase()
                   .trim()
                   .replaceAll(" ", "")
                   .replaceAll("-", "")
                   .replaceAll("/", "")
                   .replaceAll("-", "");
        if (name.equals("nt")) return "ntriples";
        if (name.equals("n3")) return "notation3";
        if (name.equals("ttl")) return "turtle";
        if (name.equals("rdf")) return "rdfxml";
        return name;
    } 

}
