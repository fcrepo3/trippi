package org.trippi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An encapsulation of an alias prefix, its long form,
 * and the relevant pattern matchers for expansion in
 * Strings for simple strings and encoded Sparql queries 
 * @author Benjamin Armintor armintor@gmail.com
 *
 */
public class Alias {
    
    private static Pattern RELATIVE_URI = Pattern.compile("<#(.+)>");
    
    private final String m_prefix;
    private final int m_prefixLen;
    private final String m_fullForm;
    
    // a lazily-instantiated regex for expanding
    private Pattern uriMatch;
    private String uriExpansion;
    
    private Pattern typeMatch;
    private String typeExpansion;
    
    public Alias(String prefix, String fullForm) {
        m_prefix = prefix;
        m_prefixLen = m_prefix.length();
        m_fullForm = fullForm;
    }
    
    public String getKey() {
        return m_prefix;
    }
    
    public String getExpansion() {
        return m_fullForm;
    }
    
    public String replaceSparqlUri(String input) {
        if (uriMatch == null) {
            uriMatch = Pattern.compile("<" + m_prefix + ":");
            uriExpansion = "<".concat(m_fullForm);
        }
        Matcher m = uriMatch.matcher(input);
        return m.replaceAll(uriExpansion);
    }
    
    public String replaceSparqlType(String input) {
        if (typeMatch == null) {
            typeMatch = Pattern.compile("\\^\\^" + m_prefix + ":(\\S+)");
            typeExpansion = "^^<" + m_fullForm + "$1>";
        }
        Matcher m = typeMatch.matcher(input);
        return m.replaceAll(typeExpansion);
    }
    
    public String replaceSimplePrefix(String input) {
        if (input.startsWith(m_prefix) && input.charAt(m_prefixLen) == ':') {
            return m_fullForm.concat(input.substring(m_prefixLen + 1));
        }
        return input;
    }

    public static String replaceRelativeUris(String input, String serverUri) {
        Matcher m = RELATIVE_URI.matcher(input);
        if (m.find()){
            StringBuilder sb = new StringBuilder(serverUri.length() + 4);
            sb.append('<').append(serverUri).append("$1>");
            return m.replaceAll(sb.toString());
        }
        return input;
    }
}
