package org.trippi.impl.mulgara;

import org.mulgara.query.Answer;
import org.mulgara.query.TuplesException;
import org.mulgara.query.Variable;

/**
 * An Answer wrapper that collapses any inner-Answers into JRDF Nodes.
 *
 * This is useful for converting a given Mulgara Answer to a simple iterator
 * of tuples.
 * <pre>
 * Example input:
 *
 * Answer { 
 *   Row {
 *     prop1 = value1
 *     prop2 = value2
 *     k0    = Answer {
 *                Row { prop3 = value3a }
 *                Row { prop3 = value3b }
 *             }
 *     k1    = Answer {
 *                Row { prop4 = value4 }
 *             }
 *   }
 * }
 *
 * Example output:
 *
 * Answer {
 *   Row {
 *     prop1 = value1
 *     prop2 = value2
 *     prop3 = value3a
 *     prop4 = value4
 *   }
 *   Row {
 *     prop1 = value1
 *     prop2 = value2
 *     prop3 = value3b
 *     prop4 = null
 *   }
 * }
 * </pre>
 */
public class CollapsedAnswer implements Answer {

    private Answer m_wrappedAnswer;

    private RowGroup m_currentRowGroup;

    private Variable[] m_variables;
    private Object[] m_values;

    public CollapsedAnswer(Answer wrappedAnswer) throws TuplesException {
        m_wrappedAnswer = wrappedAnswer;
        initialize();
    }

    /**
     * Make sure the Answer is ready to be examined.  
     *
     * A couple things need to happen here.  First,
     * the wrapped answer should be rewound with beforeFirst().
     * Next, we need to determine the names of the columns.
     * Initially, we assume that the column names are equal to those
     * provided by the wrapped answer.  But if it has at least one
     * row, we can also determine the other column names.
     */
    private void initialize() throws TuplesException {
        m_wrappedAnswer.beforeFirst();
        if (m_wrappedAnswer.next()) {
            m_currentRowGroup = new RowGroup(m_wrappedAnswer);
            m_variables = m_currentRowGroup.getVariables();
            m_values = new Object[0]; // signal to next() to try at least once
        } else {
            m_variables = m_wrappedAnswer.getVariables();
            m_values = null;
        }
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////// from org.mulgara.query.Answer ///////////////////
    ///////////////////////////////////////////////////////////////////
    
    public Object getObject(int column) throws TuplesException {
        return m_values[column];
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(String columnName) throws TuplesException {
        return getObject(getColumnIndex(columnName));
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////// from org.mulgara.query.Cursor ////////////////////
    ///////////////////////////////////////////////////////////////////


    /**
     * {@inheritDoc}
     */
    public void beforeFirst() throws TuplesException { 
       // don't do anything -- this is a one-use impl
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws TuplesException {
        m_wrappedAnswer.close();
    }

    /**
     * {@inheritDoc}
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

    /**
     * {@inheritDoc}
     */
    public Variable[] getVariables() {
        return m_variables;
    }

    /**
     * {@inheritDoc}
     * 
     * This method is unimplemented and will always throw a TuplesException.
     */
    public boolean isUnconstrained() throws TuplesException {
        throw new TuplesException("isUnconstrained() not implemented.");
    }

    /**
     * {@inheritDoc}
     * 
     * This method is unimplemented and will always throw a TuplesException.
     */
    public long getRowCount() throws TuplesException {
        throw new TuplesException("getRowCount() not implemented.");
    }

    /**
     * {@inheritDoc}
     * 
     * This method is unimplemented and will always throw a TuplesException.
     */
    public long getRowExpectedCount() throws TuplesException {
		throw new TuplesException("getRowExpectCount() not implemented.");
	}

    /**
     * {@inheritDoc}
     * 
     * This method is unimplemented and will always throw a TuplesException.
     */
    public long getRowUpperBound() throws TuplesException {
        throw new TuplesException("getRowUpperBound() not implemented.");
    }

    /**
     * {@inheritDoc}
     * 
     * This method is unimplemented and will always throw a TuplesException.
     */
    public int getRowCardinality() throws TuplesException {
        throw new TuplesException("getRowCardinality() not implemented.");
    }

    /**
     * {@inheritDoc}
     */
    public boolean next() throws TuplesException {
        if (m_values == null) return false;
        m_values = m_currentRowGroup.nextValues();
        if (m_values == null) {
            if (m_wrappedAnswer.next()) {
                m_currentRowGroup = new RowGroup(m_wrappedAnswer);
                m_variables = m_currentRowGroup.getVariables();
                m_values = m_currentRowGroup.nextValues();
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    ///////////////////////////////////////////////////////////////////
    /////////////////// from java.lang.Cloneable //////////////////////
    ///////////////////////////////////////////////////////////////////

	@Override
	public Object clone() {
        throw new RuntimeException("CollapsedAnswer.clone() not implemented.");
    }
}