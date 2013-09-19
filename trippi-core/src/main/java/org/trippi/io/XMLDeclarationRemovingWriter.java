package org.trippi.io;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;

public class XMLDeclarationRemovingWriter extends FilterWriter {

    private boolean m_buffering;
    private int m_ltCount;

    public XMLDeclarationRemovingWriter(Writer sink) {
        super(sink);
        m_buffering = true;
    }

    @Override
	public void write(int b) throws IOException {
        if (m_buffering) {
            buffer((char) b);
        } else {
            super.write(b);
        }
    }

    @Override
	public void write(char[] b) throws IOException {
        if (m_buffering) {
            for (int i = 0; i < b.length; i++) {
                buffer(b[i]);
            }
        } else {
            super.write(b);
        }
    }

    @Override
	public void write(char[] b, int off, int len) throws IOException {
        if (m_buffering) {
            for (int i = off; i < off + len; i++) {
                buffer(b[i]);
            }
        } else {
            super.write(b, off, len);
        }
    }

    private void buffer(char c) throws IOException {
        if (m_buffering) {
            if (c == '<') {
                m_ltCount++;
                if (m_ltCount == 2) {
                    m_buffering = false;
                    write(c);
                }
            }
        } else {
            write(c);
        }
    }

}
