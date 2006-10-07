package org.trippi.impl.kowari;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kowari.query.Answer;
import org.kowari.query.TuplesException;
import org.kowari.query.Variable;

/**
 * An Answer backed by an Iterator of Maps and a List of variable names, 
 * useful for testing.
 */
public class SimpleAnswer implements Answer {

    private Variable[] m_variables;
    private Iterator m_maps;
    private Map m_currentMap;

    public SimpleAnswer(Iterator maps, List names) throws TuplesException {
        m_maps = maps;
        m_variables = new Variable[names.size()];
        for (int i = 0; i < names.size(); i++) {
            m_variables[i] = new Variable((String) names.get(i));
        }
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////// from org.kowari.query.Answer ////////////////////
    ///////////////////////////////////////////////////////////////////

    public Object getObject(int column) throws TuplesException {
        return getObject(m_variables[column].getName());
    }

    public Object getObject(String columnName) throws TuplesException {
        return m_currentMap.get(columnName);
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////// from org.kowari.query.Cursor ////////////////////
    ///////////////////////////////////////////////////////////////////


    public void beforeFirst() throws TuplesException { }

    public void close() throws TuplesException { }

    /**
     * Find the index of a variable.
     *
     * @return the ColumnIndex value
     */
    public int getColumnIndex(Variable column) throws TuplesException {
        return getColumnIndex(column.getName());
    }

    private int getColumnIndex(String name) throws TuplesException {
        for (int i = 0; i < m_variables.length; i++) {
            if (m_variables[i].getName().equals(name)) return i;
        }
        return -1;
    }

    /**
     * Return the number of variables (columns).
     */
    public int getNumberOfVariables() {
        return m_variables.length;
    }

    public Variable[] getVariables() {
        return m_variables;
    }

    public boolean isUnconstrained() throws TuplesException {
        throw new TuplesException("isUnconstrained() not implemented.");
    }

    public long getRowCount() throws TuplesException {
        throw new TuplesException("getRowCount() not implemented.");
    }


    public long getRowUpperBound() throws TuplesException {
        throw new TuplesException("getRowUpperBound() not implemented.");
    }

    public int getRowCardinality() throws TuplesException {
        throw new TuplesException("getRowCardinality() not implemented.");
    }

    /**
     * Move to the next row. 
     *
     * If no such row exists, return false and the current row becomes 
     * unspecified. The current row is unspecified when an instance is 
     * created. To specify the current row, the beforeFirst() method must 
     * be invoked.
     *
     * @return whether a subsequent row exists.
     */
    public boolean next() throws TuplesException {
        if (m_maps.hasNext()) {
            m_currentMap = (Map) m_maps.next();
            return true;
        } else {
            return false;
        }
    }

    ///////////////////////////////////////////////////////////////////
    /////////////////// from java.lang.Cloneable //////////////////////
    ///////////////////////////////////////////////////////////////////

    public Object clone() {
        throw new RuntimeException("SimpleAnswer.clone() not implemented.");
    }

}