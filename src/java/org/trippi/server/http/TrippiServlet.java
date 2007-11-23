package org.trippi.server.http;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.config.TrippiConfig;
import org.trippi.config.TrippiProfile;
import org.trippi.server.TrippiServer;

/**
 * A Java servlet that exposes <code>TrippiServer</code>(s) via HTTP.
 * <p>
 * The required servlet initialization parameter, <b>configFile</b> specifies 
 * the full path to the trippi.config file.  When responding to a call at the
 * base URL of this servlet, all profiles in the configFile will be listed.
 * They can be accessed separately at baseURL/profileId.
 * </p><p>
 * The optional parameter, <b>profileId</b>, indicates a particular triplestore
 * to expose.  If specified, this changes the behavior of this servlet
 * so that only one triplestore is exposed.  It is accessed directly at
 * the baseURL of this servlet.
 * </p>
 * @author cwilper@cs.cornell.edu
 */
public class TrippiServlet 
        extends HttpServlet {

    //////////////////////////////////////////////////////////////////////////
    // Used in single-server mode.
    //////////////////////////////////////////////////////////////////////////

    private static final long serialVersionUID = 1L;

    /**
     * The connector to expose.
     */
    private TriplestoreConnector m_connector;

    /**
     * The server instance.
     */
    private TrippiServer m_server;

    //////////////////////////////////////////////////////////////////////////
    // Used in multi-server mode.
    //////////////////////////////////////////////////////////////////////////

    /** 
     * Map of <code>TriplestoreConnector</code>s 
     * keyed by <code>TrippiProfile</code>s.
     */
    private Map<TrippiProfile, TriplestoreConnector> m_connectors;

    /**
     * Map of <code>TrippiServer</code>s keyed by profile id.
     */
    private Map<String, TrippiServer> m_servers;

    /**
     * Stylesheet transformer for index, form, and error pages.
     */
    private Styler m_styler;

    //////////////////////////////////////////////////////////////////////////
    // Initialization methods
    //////////////////////////////////////////////////////////////////////////

    public TriplestoreReader getReader() throws ServletException {
        return null;
    }
    
    public TriplestoreWriter getWriter() throws ServletException {
        return null;
    }

    /**
     * Get the single connector that this servlet is configured to expose,
     * pre-configured with aliases.
     *
     * Otherwise, return null.
     */
    public TriplestoreConnector getConnector() throws ServletException {
        String profileId = getInitParameter("profileId");
        if (profileId == null) return null;
        String configFile = getInitParameter("configFile");
        if (configFile != null && !configFile.equals("")) {
            try {
                TrippiConfig config = new TrippiConfig(new File(configFile));
                TrippiProfile profile = (TrippiProfile) config.getProfiles().get(profileId);
                if (profile != null) {
                    TriplestoreConnector conn = profile.getConnector();
                    conn.getReader().setAliasMap(config.getAliasMap());
                    return conn;
                } else {
                    throw new TrippiException("No such profile: " + profileId);
                }
            } catch (Exception e) {
                throw new ServletException("Error initializing in single-server mode.", e);
            }
        } else {
            throw new ServletException("configFile initialization parameter missing.");
        }
    }

    /**
     * Get all connectors that this servlet is configured to expose, pre-configured
     * with aliases.
     *
     * This will be a Map, with <code>TrippiProfile</code> objects as keys.
     */
    private Map<TrippiProfile, TriplestoreConnector> getConnectors() throws ServletException {
        String configFile = getInitParameter("configFile");
        if (configFile != null && !configFile.equals("")) {
            Map<TrippiProfile, TriplestoreConnector> connectors = new HashMap<TrippiProfile, TriplestoreConnector>();
            try {
                TrippiConfig config = new TrippiConfig(new File(configFile));
                Map profiles = config.getProfiles();
                Iterator iter = profiles.keySet().iterator();
                while (iter.hasNext()) {
                    TrippiProfile profile = (TrippiProfile) profiles.get(iter.next());
                    TriplestoreConnector conn = profile.getConnector();
                    conn.getReader().setAliasMap(config.getAliasMap());
                    connectors.put(profile, conn);
                }
                return connectors;
            } catch (Exception e) {
                // clean up any connectors that have been opened
                Iterator<TrippiProfile> iter = connectors.keySet().iterator();
                while (iter.hasNext()) {
                    TrippiProfile profile = (TrippiProfile) iter.next();
                    TriplestoreConnector conn = (TriplestoreConnector) connectors.get(profile);
                    try {
                        conn.close();
                    } catch (TrippiException e2) {
                        log("Error closing connector", e2);
                    }
                }
                throw new ServletException("Error initializing in multi-server mode.", e);
            }
        } else {
            throw new ServletException("configFile initialization parameter missing.");
        }
    }

    /**
     * Override this method to return false if the connector(s) should not
     * be closed when the servlet is stopped.
     */
    public boolean closeOnDestroy() { return true; }

    // implementations can return something different... this will affect the
    // value of the root "context" element on the error, index, and form pages.
    // this value comes in handy for stylesheets that need to make references 
    // to other things.
    public String getContext(String origContext) {
        return origContext;  
    }

    // implementations can override to return anything.
    // these should be /web/paths/like/this.xsl
    public String getErrorStylesheetLocation() {
        return getInitParameter("errorStylesheetLocation");
    }

    public String getIndexStylesheetLocation() {
        return getInitParameter("indexStylesheetLocation");
    }

    public String getFormStylesheetLocation() {
        return getInitParameter("formStylesheetLocation");
    }

    ////////////////////////////////////////////////////////////////////////

    private String getPath(String loc) {
        if (loc == null) return null;
        if (loc.startsWith("/")) {
            String foo = getServletContext().getRealPath("/foo");
            File dir = new File(foo).getParentFile().getParentFile();
            File file = new File(dir, loc);
            return file.toString();
        } else {
            return getServletContext().getRealPath(loc);
        }
    }

    /**
     * Initialize the servlet.
     */
    public final void init() throws ServletException {
        // first load the styles
        try {
            String indexStylesheetPath = getPath(getIndexStylesheetLocation());
            String formStylesheetPath = getPath(getFormStylesheetLocation());
            String errorStylesheetPath = getPath(getErrorStylesheetLocation());
            m_styler = new Styler(indexStylesheetPath,
                                  formStylesheetPath,
                                  errorStylesheetPath);
        } catch (Exception e) {
            throw new ServletException("Error loading stylesheet(s)", e);
        }
        // then init whichever kind of accessor we're going to use
        TriplestoreWriter writer = getWriter();
        if (writer != null) {
            m_server = new TrippiServer(writer);
            return;
        }
        
        TriplestoreReader reader = getReader();
        if (reader != null) {
            m_server = new TrippiServer(reader);
            return;
        }
        
        m_connector = getConnector();
        if (m_connector != null) {
            m_server = new TrippiServer(m_connector);
        } else {
            m_connectors = getConnectors();
            // build the id-to-server map
            m_servers = new HashMap<String, TrippiServer>();
            Iterator<TrippiProfile> iter = m_connectors.keySet().iterator();
            while (iter.hasNext()) {
                TrippiProfile profile = iter.next();
                TriplestoreConnector conn = (TriplestoreConnector) m_connectors.get(profile);
                m_servers.put(profile.getId(), new TrippiServer(conn));
            }
        }
    }

    /**
     * Dispatch the request to the appropriate server.
     *
     * If no server is specified in the request URI, and the servlet is running
     * in multi-server mode, show an index of servers.
     */
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response) 
              throws ServletException, IOException {
    	request.setCharacterEncoding("UTF-8");
        try {
            // first decide which server to use for the response
            String profileId = request.getPathInfo();
            if (profileId != null) profileId = profileId.replaceAll("/", "");
            if (profileId == null || profileId.equals("")) {
                if (m_server != null) {
                    doGet(m_server, request, response);
                } else {
                    response.setContentType("text/html; charset=UTF-8");
                    doIndex(
                        new PrintWriter(
                            new OutputStreamWriter(response.getOutputStream(), 
                                                   "UTF-8")),
                        request.getRequestURL().toString(),
                        request.getContextPath());
                }
            } else {
                if (m_server != null) {
                    throw new ServletException("Not in multi-server mode.");
                } else {
                    doGet((TrippiServer) m_servers.get(profileId), request, response);
                }
            }
        } catch (ServletException e) {
            throw e;
        } catch (Throwable th) {
            try {
                response.setContentType("text/html; charset=UTF-8");
                response.setStatus(500);
                StringWriter sWriter = new StringWriter();
                PrintWriter out = new PrintWriter(sWriter);
                out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                out.println("<error context=\"" + enc(getContext(request.getContextPath())) + "\">");
                out.println("<message>" + enc(getLongestMessage(th, "Error")) + "</message>");
                out.print("<detail><![CDATA[");
                th.printStackTrace(out);
                out.println("]]></detail>");
                out.println("</error>");
                out.flush();
                out.close();
                PrintWriter reallyOut = new PrintWriter(
                                      new OutputStreamWriter(
                                          response.getOutputStream(), "UTF-8"));
                m_styler.sendError(sWriter.toString(), reallyOut);
                reallyOut.flush();
                reallyOut.close();
            } catch (Exception e2) {
                log("Error sending error response to browser.", e2);
                throw new ServletException(th);
            }
        }
    }

    private String enc(String in) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            if (c == '<') {
                out.append("&lt;");
            } else if (c == '>') {
                out.append("&gt;");
            } else if (c == '\'') {
                out.append("&apos;");
            } else if (c == '"') {
                out.append("&quot;");
            } else if (c == '&') {
                out.append("&amp;");
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private String getLongestMessage(Throwable th, String longestSoFar) {
        if (th.getMessage() != null && th.getMessage().length() > longestSoFar.length()) {
            longestSoFar = th.getMessage();
        }
        Throwable cause = th.getCause();
        if (cause == null) return longestSoFar;
        return getLongestMessage(cause, longestSoFar);
    }

    public void doGet(TrippiServer server, 
                      HttpServletRequest request,
                      HttpServletResponse response)
            throws Exception {
        if (server == null) {
            throw new ServletException("No such triplestore.");
        }
        String type = request.getParameter("type");
        String template = request.getParameter("template");
        String lang = request.getParameter("lang");
        String query = request.getParameter("query");
        String limit = request.getParameter("limit");
        String distinct = request.getParameter("distinct");
        String format = request.getParameter("format");
        String dumbTypes = request.getParameter("dt");
        String stream = request.getParameter("stream");
        boolean streamImmediately = (stream != null) && (stream.toLowerCase().startsWith("t") || stream.toLowerCase().equals("on"));
        String flush = request.getParameter("flush");
        if (type == null && template == null && lang == null && query == null && limit == null && distinct == null && format == null) {
        	if (flush == null || flush.equals("")) flush = "false";
            boolean doFlush = flush.toLowerCase().startsWith("t");
            if (doFlush) {
            	TriplestoreWriter writer = m_server.getWriter();
            	if (writer != null) writer.flushBuffer();
            }
        	response.setContentType("text/html; charset=UTF-8");
            doForm(server, new PrintWriter(new OutputStreamWriter(
                                   response.getOutputStream(), "UTF-8")),
                           request.getRequestURL().toString(),
                           request.getContextPath());
        } else {
            doFind(server, type, template, lang, query, limit, distinct, format, dumbTypes, streamImmediately, flush, response);
        }
    }

    public void doIndex(PrintWriter out, String requestURI, String contextPath)
              throws Exception {
        try {
            StringWriter sWriter = new StringWriter();
            PrintWriter sout = new PrintWriter(sWriter);
            sout.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            String href = enc(requestURI.replaceAll("/$", ""));
            sout.println("<trippi-server href=\"" + href + "\" context=\"" + enc(getContext(contextPath)) + "\">");
            Iterator<TrippiProfile> iter = m_connectors.keySet().iterator();
            while (iter.hasNext()) {
                TrippiProfile profile = iter.next();
                sout.println("  <profile id=\"" + profile.getId() 
                        + "\" label=\"" + enc(profile.getLabel())
                        + "\" connector=\"" + profile.getConnectorClassName() + "\">");
                Map<String, String> config = profile.getConfiguration();
                Iterator<String> names = config.keySet().iterator();
                while (names.hasNext()) {
                    String name = names.next();
                    String value = config.get(name);
                    sout.println("    <param name=\"" + name + "\" value=\"" + enc(value) + "\"/>");
                }
                sout.println("  </profile>");
            }
            sout.println("</trippi-server>");
            sout.flush();
            m_styler.sendIndex(sWriter.toString(), out);
        } finally {
            try {
                out.flush();
                out.close();
            } catch (Exception ex) {
                log("Error closing response", ex);
            }
        }
    }

    public void doForm(TrippiServer server,
                          PrintWriter out,
                          String requestURI,
                          String contextPath)
              throws Exception {
        try {
            StringWriter sWriter = new StringWriter();
            PrintWriter sout = new PrintWriter(sWriter);
            sout.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            TriplestoreReader reader = server.getReader(); 
            String href = enc(requestURI.replaceAll("/$", ""));
            sout.println("<query-service href=\"" + href + "\" context=\"" + enc(getContext(contextPath)) + "\">");
            sout.println("  <alias-map>");
            Map<String, String> map = reader.getAliasMap();
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String name = iter.next();
                String uri = map.get(name);
                sout.println("    <alias name=\"" + name + "\" uri=\"" + enc(uri) + "\"/>");
            }
            sout.println("  </alias-map>");
            sout.println("  <triple-languages>"); 
            String[] langs = reader.listTripleLanguages();
            for (int i = 0; i < langs.length; i++) {
                sout.println("    <language name=\"" + enc(langs[i]) + "\"/>");
            }
            sout.println("  </triple-languages>"); 
            langs = reader.listTupleLanguages();
            sout.println("  <tuple-languages>"); 
            for (int i = 0; i < langs.length; i++) {
                sout.println("    <language name=\"" + enc(langs[i]) + "\"/>");
            }
            sout.println("  </tuple-languages>"); 
            sout.println("  <triple-output-formats>");
            RDFFormat[] formats = TripleIterator.OUTPUT_FORMATS;
            for (int i = 0; i < formats.length; i++) {
                sout.println("    <format name=\"" + enc(formats[i].getName()) 
                        + "\" encoding=\"" + formats[i].getEncoding()
                        + "\" media-type=\"" + formats[i].getMediaType()
                        + "\" extension=\"" + formats[i].getExtension() + "\"/>");
            }
            sout.println("  </triple-output-formats>");
            sout.println("  <tuple-output-formats>");
            formats = TupleIterator.OUTPUT_FORMATS;
            for (int i = 0; i < formats.length; i++) {
                sout.println("    <format name=\"" + enc(formats[i].getName()) 
                        + "\" encoding=\"" + formats[i].getEncoding()
                        + "\" media-type=\"" + formats[i].getMediaType()
                        + "\" extension=\"" + formats[i].getExtension() + "\"/>");
            }
            sout.println("  </tuple-output-formats>");
            sout.println("</query-service>");
            sout.flush();
            m_styler.sendForm(sWriter.toString(), out);
        } finally {
            try {
                out.flush();
                out.close();
            } catch (Exception ex) {
                log("Error closing response", ex);
            }
        }
    }

    public void doFind(TrippiServer server,
                          String type, 
                          String template, 
                          String lang, 
                          String query, 
                          String limit, 
                          String distinct, 
                          String format, 
                          String dumbTypes,
                          boolean streamImmediately,
						  String flush,
                          HttpServletResponse response) throws Exception {
        OutputStream out = null;
        File tempFile = null;
        try {        	
            if (streamImmediately) {
                String mediaType = 
                        TrippiServer.getResponseMediaType(format,
                                                          !(type != null && type.equals("triples")),
                                                          TrippiServer.getBoolean(dumbTypes, false));
                try {
                    response.setContentType(mediaType + "; charset=UTF-8");
                    out = response.getOutputStream();
                    server.find(type, template, lang, query, limit, distinct, format, dumbTypes, flush, out);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new ServletException("Error querying", e);
                }
            } else {
                tempFile = File.createTempFile("trippi", "result");
                FileOutputStream tempOut = new FileOutputStream(tempFile);
                String mediaType = server.find(type, template, lang, query, limit, distinct, format, dumbTypes, flush, tempOut);
                tempOut.close();
                response.setContentType(mediaType + "; charset=UTF-8");
                out = response.getOutputStream();
                FileInputStream results = new FileInputStream(tempFile);
                sendStream(results, out);
            }
        } finally {
            // make sure the response stream is closed and the tempfile is deld
            if (out != null) try { out.close(); } catch (Exception e) { }
            if (tempFile != null) tempFile.delete();
        }
    }

    private void sendStream(InputStream in, OutputStream out) throws IOException {
        try {
            byte[] buf = new byte[4096];
            int len;
            while ( ( len = in.read( buf ) ) > 0 ) {
                out.write( buf, 0, len );
            }
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                log("Could not close result inputstream.");
            }
        }
    }

    /**
     * Close the connector instance when cleaning up if closeOnDestroy().
     */
    public void destroy() {
        if (closeOnDestroy()) {
            if (m_connector != null) {
                // single-server mode
                try {
                    m_connector.close();
                } catch (Exception e) {
                    log("Error closing connector", e);
                }
            } else if (m_connectors != null) {
                // multi-server mode
                Iterator<TriplestoreConnector> iter = m_connectors.values().iterator();
                while (iter.hasNext()) {
                    TriplestoreConnector conn = (TriplestoreConnector) iter.next();
                    try {
                        conn.close();
                    } catch (Exception e) {
                        log("Error closing connector", e);
                    }
                }
            }
        }
    }

    /** Exactly the same behavior as doGet. */
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }

}
