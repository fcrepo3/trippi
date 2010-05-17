package org.trippi.impl.count;

import java.util.HashMap;
import java.util.Map;

import org.jrdf.graph.Node;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class CountTupleIterator extends TupleIterator {
    public static final String[] NAMES = new String[]{"count"};

    private boolean m_hasNext;
    private boolean m_isClosed;
    private final TupleIterator m_src;
    public CountTupleIterator(TupleIterator iter){
    	m_isClosed = false;
    	m_hasNext = true;
    	m_src = iter;
    	this.setCallback(m_src.getCallback());       
    }
    
    @Override
    public boolean hasNext() {
        return m_hasNext;
    }

    @Override
    public Map<String, Node> next() throws TrippiException {
        if (!m_hasNext) return null;
        try{
            Node value = new CountLiteral(m_src.count());
            Map<String, Node> m_value_map = new HashMap<String, Node>();
            m_value_map.put(NAMES[0], value);
        	return m_value_map;
        }
        finally{
            m_hasNext = false;
        }
    }

    @Override
    public String[] names() throws TrippiException {
        return NAMES;
    }

    @Override
    public void close() throws TrippiException {
        if (!m_isClosed) {
            m_isClosed = true;
            m_hasNext = false;
            m_src.close();
        }
    }

    public void finalize() throws TrippiException {
        close();
    }

}
