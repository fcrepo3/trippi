package org.trippi.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.trippi.TripleIterator;
import org.trippi.TrippiException;

/**
 * TripleWriter extension to simply write to the OutputStream the number of
 * triples in the given TripleIterator.
 *
 * @author B J O'Steen
 */
public class CountTripleWriter extends TripleWriter {

	private Writer _out;

	public CountTripleWriter(OutputStream out) {
		this( new OutputStreamWriter(out) );
	}

	public CountTripleWriter(Writer out) {
		_out = out;
	}

    @Override
	public int write(TripleIterator iter) throws TrippiException {
        try {
            /* Use the in-built TripleIterator.count() method */
            int count = iter.count();
            
            /* Finger's crossed... */            
            String str_count = Integer.toString(count);
            
            _out.write( str_count );
            
            /* Flush to OutputStream */
            _out.flush();
            
            /* Documentation states that the TripleWriter should close the 
               Iterator, but not the stream, hence: */
            iter.close();
            return count;
        } catch (IOException e) {
            throw new TrippiException("Error setting up CountTripleWriter", e);
        } 
    }
}
