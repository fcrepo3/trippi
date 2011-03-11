package org.trippi.server.http;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.StringReader;

import javax.xml.transform.Templates;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/**
 * Utility for performing server-side xsl transformations.
 *
 * @author cwilper@cs.cornell.edu
 */
public class Styler {

    /** the file path of the stylesheet to use for index transformations. */
    private final String m_indexStyle;

    /** the file path of the stylesheet to use for form transformations. */
    private final String m_formStyle;

    /** the file path of the stylesheet to use for error transformations. */
    private final String m_errorStyle;

    /** the loaded index stylesheet. */
    private Templates m_indexTemplates;

    /** the loaded form stylesheet. */
    private Templates m_formTemplates;

    /** the loaded error stylesheet. */
    private Templates m_errorTemplates;

    /** provider of per-thread transformers. */
    private TransformerFactory m_factory;

    /** 
     * Construct a Styler to do transformations.
     *
     * Each parameter is optional (may be given as null), but the caller is 
     * responsible for not calling the corresponding send() method.
     */
    public Styler(String indexStyle,
                  String formStyle,
                  String errorStyle) throws Exception {
        m_indexStyle = indexStyle;
        m_formStyle = formStyle;
        m_errorStyle = errorStyle;
        m_factory = TransformerFactory.newInstance();
        reload();
    }

    /**
     * Transform the given xml using the index stylesheet, outputting to out.
     */
    public void sendIndex(String xml, PrintWriter out) throws Exception {
        send(xml, out, m_indexTemplates);
    }

    /**
     * Transform the given xml using the form stylesheet, outputting to out.
     */
    public void sendForm(String xml, PrintWriter out) throws Exception {
        send(xml, out, m_formTemplates);
    }

    /**
     * Transform the given xml using the error stylesheet, outputting to out.
     */
    public void sendError(String xml, PrintWriter out) throws Exception {
        send(xml, out, m_errorTemplates);
    }

    /**
     * Reload the stylesheets.
     */
    public synchronized void reload() throws Exception {
        if (m_indexStyle != null) m_indexTemplates = load(m_indexStyle); 
        if (m_formStyle != null) m_formTemplates = load(m_formStyle); 
        if (m_errorStyle != null) m_errorTemplates = load(m_errorStyle); 
    }

    // load the referenced stylesheet into a Templates object and return it.
    private Templates load(String filePath) throws Exception {
        return m_factory.newTemplates(new StreamSource(new FileInputStream(filePath)));
    }

    // do a transformation and send it to the stream
    private static void send(String source,
                             PrintWriter out, 
                             Templates stylesheet) throws Exception {
        stylesheet.newTransformer()
                  .transform(new StreamSource(new StringReader(source)), 
                             new StreamResult(out));
    }
                             
}
