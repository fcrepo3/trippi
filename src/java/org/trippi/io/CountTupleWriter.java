package org.trippi.io;

import java.io.IOException;

import org.trippi.TupleIterator;
import org.trippi.TrippiException;

import java.io.OutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * TupleWriter extension to simply write to the OutputStream the number of
 * tuple rows in the given TupleIterator.
 *
 * @author B J O'Steen
 */
public class CountTupleWriter extends TupleWriter {

	private Writer _out;

	public CountTupleWriter(OutputStream out) {
		this( new OutputStreamWriter(out) );
	}

	public CountTupleWriter(Writer out) {
		_out = out;
	}

    public int write(TupleIterator iter) throws TrippiException {
        try {
            /* Use the in-built TupleIterator.count() method */
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
            throw new TrippiException("Error setting up CountTupleWriter", e);
        } 
    }
}
