package org.trippi.impl.kowari;

import org.kowari.query.Answer;
import org.kowari.query.TuplesException;
import org.kowari.query.Variable;

/**
 * An Answer wrapper that collapses any inner-Answers into JRDF Nodes.
 *
 * This is useful for converting a given kowari Answer to a simple iterator
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
            // System.out.println("Wrapped answer has at least one row");
            m_currentRowGroup = new RowGroup(m_wrappedAnswer);
            m_variables = m_currentRowGroup.getVariables();
            m_values = new Object[0]; // signal to next() to try at least once
        } else {
            // System.out.println("Wrapped answer has no rows");
            m_variables = m_wrappedAnswer.getVariables();
            m_values = null;
        }
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////// from org.kowari.query.Answer ////////////////////
    ///////////////////////////////////////////////////////////////////

    public Object getObject(int column) throws TuplesException {
        return m_values[column];
    }

    public Object getObject(String columnName) throws TuplesException {
        return getObject(getColumnIndex(columnName));
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////// from org.kowari.query.Cursor ////////////////////
    ///////////////////////////////////////////////////////////////////


    /**
     * Reset to iterate through every single element.
     */
    public void beforeFirst() throws TuplesException { 
       // don't do anything -- this is a one-use impl
    }

    /**
     * Free resources associated with this instance.
     */
    public void close() throws TuplesException {
        m_wrappedAnswer.close();
    }

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

    /**
     * Get the variables bound and their default collation order. 
     *
     * The array returned by this method should be treated as if its contents 
     * were immutable, even though Java won't enforce this. If the elements of 
     * the array are modified, there may be side effects on the past and 
     * future clones of the tuples it was obtained from.
     *
     * @return the Variables bound within this answer
     */
    public Variable[] getVariables() {
        return m_variables;
    }

    /**
     * Test whether this is a unit-valued answer. 
     *
     * A unit answer appended to something yields the unit answer. A unit 
     * answer joined to something yields the same something. Notionally, the 
     * unit answer has zero columns and one row.
     *
     * @return true if the answer is unconstrained
     */
    public boolean isUnconstrained() throws TuplesException {
        throw new TuplesException("isUnconstrained() not implemented.");
    }

    /**
     * Return the exact number of rows this instance contains.
     */
    public long getRowCount() throws TuplesException {
        throw new TuplesException("getRowCount() not implemented.");
    }


    /**
     * Return an upper bound on the number of rows this instance contains.
     */
    public long getRowUpperBound() throws TuplesException {
        throw new TuplesException("getRowUpperBound() not implemented.");
    }

    /**
     * Return cardinality of the number of rows which this instance contains.
     *
     * @return the cardinality of this tuples. {0,1,N} rows.
     */
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

    public Object clone() {
        throw new RuntimeException("CollapsedAnswer.clone() not implemented.");
    }
}