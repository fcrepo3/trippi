package org.trippi.io;


public abstract class RDFWriter {

    /**
     * XML-encode the given string.
     */
    public static String enc(String s) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '<') {
                out.append("&lt;");
            } else if (c == '>') {
                out.append("&gt;");
            } else if (c == '"') {
                out.append("&quot;");
            } else if (c == '\'') {
                out.append("&apos;");
            } else if (c == '&') {
                out.append("&amp;");
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
