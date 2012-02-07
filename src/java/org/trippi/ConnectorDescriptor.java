package org.trippi;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConnectorDescriptor {

    private String m_name;
    private String m_description;
    private List<ConnectorParameter> m_parameters;

    public static ConnectorDescriptor forName(String className) 
            throws TrippiException {
        String path = className.replaceAll("\\.", "/") + "Descriptor.xml";
        InputStream xml = ClassLoader.getSystemClassLoader().
                          getResourceAsStream(path);
        if (xml != null) {
            return new ConnectorDescriptor(xml);
        }
        throw new TrippiException("Not found in classpath: " + path);
    }

    public static Map<String, ConnectorDescriptor> find() throws TrippiException,
                                    IOException {
        Map<String, ConnectorDescriptor> m = new HashMap<String, ConnectorDescriptor>();
        File jarDir = new File(System.getProperty("java.endorsed.dirs"));
        String[] jarNames = jarDir.list();
        for (int i = 0; i < jarNames.length; i++) {
            if (jarNames[i].toLowerCase().endsWith(".jar")) {
                JarFile f = new JarFile(new File(jarDir, jarNames[i]));
                Enumeration<JarEntry> e = f.entries();
                while (e.hasMoreElements()) {
                    JarEntry entry = e.nextElement();
                    if (entry.getName().endsWith("Descriptor.xml")) {
                        String c = entry.getName().replaceAll("Descriptor.xml", "");
                        if (f.getJarEntry(c + ".class") != null) {
                            String className = c.replaceAll("/", ".");
                            m.put(className, TriplestoreConnector.getDescriptor(className));
                        }
                    }
                }
            }
        }
        return m;
    }

    public ConnectorDescriptor(InputStream in) throws TrippiException {
        m_parameters = new ArrayList<ConnectorParameter>();
        try {
            Element root = DocumentBuilderFactory.newInstance().
                                   newDocumentBuilder().parse(in).
                                   getDocumentElement();
            m_name = root.getAttribute("name");
            m_description = getDescription(root);
            m_parameters = getParameters(root);        
        } catch (Exception e) {
            throw new TrippiException("Unexpected error in descriptor.xml", e);
        }
        if (m_name == null || m_name.equals("")) 
                throw new TrippiException("Bad descriptor.xml: name "
                        + "attribute required on root element.");
    }

    private String getDescription(Element parent) throws Exception {
        NodeList d = parent.getChildNodes();
        for (int i = 0; i < d.getLength(); i++) {
            Node n = d.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE && n.getNodeName().equals("description")) {
                String val = n.getFirstChild().getNodeValue();
                if (val != null)
                    return val.trim().replaceAll("\n", " ").
                                      replaceAll(" +", " ");
            }
        }
        return null;
    }

    private List<ConnectorParameter> getParameters(Element parent) throws Exception {
        NodeList d = parent.getChildNodes();
        List<ConnectorParameter> paramList = new ArrayList<ConnectorParameter>();
        for (int i = 0; i < d.getLength(); i++) {
            Node n = d.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE && n.getNodeName().equals("parameter")) {
                paramList.add(getParameter((Element) n));
            }
        }
        return paramList;
    }

    private ConnectorParameter getParameter(Element param) throws Exception {
        String name = param.getAttribute("name");
        String label = param.getAttribute("label");
        String optional = param.getAttribute("optional");
        boolean isOptional = true;
        if (optional == null || optional == "" 
                || optional.equalsIgnoreCase("false") 
                || optional.equalsIgnoreCase("no")) {
            isOptional = false;
        }
        String description = getDescription(param);
        Map<String, List<ConnectorParameter>> paramsMap = new HashMap<String, List<ConnectorParameter>>();
        List<String> options = new ArrayList<String>();
        NodeList d = param.getChildNodes();
        for (int i = 0; i < d.getLength(); i++) {
            Node n = d.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE && n.getNodeName().equals("option")) {
                Element option = (Element) n;
                String optionValue = option.getAttribute("value");
                List<ConnectorParameter> optionParameters = getParameters(option);
                options.add(optionValue); // purpose: preserves order
                paramsMap.put(optionValue, optionParameters);
            }
        }
        return new ConnectorParameter(name, label, description, 
                                      isOptional, options, paramsMap);
    }

    public String getName() {
        return m_name;
    }

    public String getDescription() {
        return m_description;
    }

    /**
     * Get the list of <code>ConnectorParameter</code>s that together describe 
     * the configuration options for the connector.
     */
    public List<ConnectorParameter> getParameters() {
        return m_parameters;
    }

    @Override
	public String toString() {
        StringBuffer out = new StringBuffer();
        out.append("Connector name : " + m_name + "\n");
        out.append("   Description : " + m_description + "\n");
        Iterator<ConnectorParameter> iter = m_parameters.iterator();
        while (iter.hasNext()) {
            ConnectorParameter param = iter.next();
            out.append(param.toString(0));
        }
        return out.toString();
    }

    public static void main(String[] args) throws Exception {
        ConnectorDescriptor d = new ConnectorDescriptor(new FileInputStream(new File(args[0])));
        System.out.println(d.toString());
    }

}
