package org.trippi.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class XMLDeclarationRemover extends FilterOutputStream {

    private boolean m_buffering;
    private int m_ltCount;

    public XMLDeclarationRemover(OutputStream sink) {
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
	public void write(byte[] b) throws IOException {
        if (m_buffering) {
            for (int i = 0; i < b.length; i++) {
                buffer((char) b[i]);
            }
        } else {
            super.write(b);
        }
    }

    @Override
	public void write(byte[] b, int off, int len) throws IOException {
        if (m_buffering) {
            for (int i = off; i < off + len; i++) {
                buffer((char) b[i]);
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
