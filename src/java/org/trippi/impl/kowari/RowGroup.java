package org.trippi.impl.kowari;

import java.util.ArrayList;
import java.util.List;

import org.kowari.query.Answer;
import org.kowari.query.TuplesException;
import org.kowari.query.Variable;

/**
 * An iterator-like structure for turning one row of any Answer into one
 * or more rows, depending on the existence of an inner-Answer.
 */
public class RowGroup {

    private Answer m_answer;

    private Object[] m_nextValues;

    private boolean m_exhausted;
    private int m_rowCount;

    private Variable[] m_variables;
    private List m_valueList;

    /**
     * The given answer must be posititioned on a row.
     */
    public RowGroup(Answer answer) throws TuplesException {
        m_answer = answer;
        m_exhausted = false;
        m_rowCount = 0;
        initialize();
    }

    /**
     * Make the valueList while coming up with the list of variables 
     * in the order in which they occur.
     */
    private void initialize() throws TuplesException {
        Variable[] origVars = m_answer.getVariables();
        List newVars = new ArrayList();
        m_valueList = new ArrayList();
        for (int i = 0; i < origVars.length; i++) {
            Object val = m_answer.getObject(i);
            if (val instanceof Answer) {
                Answer nonCollapsedAnswer = (Answer) val;
                nonCollapsedAnswer.beforeFirst();
                Answer innerAnswer = new CollapsedAnswer(nonCollapsedAnswer);
                m_valueList.add(innerAnswer);
                Variable[] innerVars = innerAnswer.getVariables();
                for (int j = 0; j < innerVars.length; j++) {
                    newVars.add(innerVars[j]);
                }
            } else {
                m_valueList.add(val);
                newVars.add(origVars[i]);
            }
        }
        m_variables = new Variable[newVars.size()];
        for (int i = 0; i < newVars.size(); i++) {
            m_variables[i] = (Variable) newVars.get(i);
        }
        m_nextValues = new Object[m_variables.length];
    }


    /**
     * Return the variables for this rowgroup.
     */
    public Variable[] getVariables() throws TuplesException {
        return m_variables;
    }

    /**
     * Return an object array or null if exhausted.
     */
    public Object[] nextValues() throws TuplesException {
        setNext();
        return m_nextValues;
    }

    /**
     * Set m_nextValues to the next values (or null if exhausted).
     */
    private void setNext() throws TuplesException {
        if (!m_exhausted) {
            boolean atLeastOneInnerAnswerWasNotExhausted = false;
            int c = 0;
            for (int i = 0; i < m_valueList.size(); i++) {
                Object val = m_valueList.get(i);
                if (val instanceof Answer) {
                    Answer innerAnswer = (Answer) val;
                    boolean hadNext = innerAnswer.next();
                    for (int j = 0; j < innerAnswer.getNumberOfVariables(); j++) {
                        if (hadNext) {
                            m_nextValues[c++] = innerAnswer.getObject(j);
                        } else {
                            m_nextValues[c++] = null;
                        }
                    }
                    if (hadNext) atLeastOneInnerAnswerWasNotExhausted = true;
                } else {
                    m_nextValues[c++] = val;
                }
            }
            if (!atLeastOneInnerAnswerWasNotExhausted) {
                m_exhausted = true;
                if (m_rowCount > 0) {
                    m_nextValues = null;
                }
            }
        } else {
            m_nextValues = null;
        }
        m_rowCount++;
    }

}