package org.trippi.impl.kowari;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kowari.query.Answer;
import org.kowari.query.Variable;

public class CollapsedAnswerTest {

    private boolean m_printOriginal;
    private boolean m_printCollapsed;

    public CollapsedAnswerTest() {
        m_printOriginal = true;
        m_printCollapsed = true;
    }

    public void testNoRows() throws Exception {
        List names = new ArrayList();
        names.add("prop1");
        names.add("prop2");
        doTest(new ArrayList(), names);
    }

    public void testTwoRows() throws Exception {
        List names = new ArrayList();
        names.add("prop1");
        names.add("prop2");
        List maps = new ArrayList();
        Map row1 = new HashMap();
        row1.put("prop1", "row 1, value 1");
        row1.put("prop2", "row 1, value 2");
        Map row2 = new HashMap();
        row2.put("prop1", "row 2, value 1");
        row2.put("prop2", "row 2, value 2");
        maps.add(row1);
        maps.add(row2);
        doTest(maps, names);
    }

    public void testComplexRows() throws Exception {
        List names = new ArrayList();
        names.add("prop1");
        names.add("prop2");
        names.add("k0");
        names.add("k1");
        List maps = new ArrayList();
        Map row1 = new HashMap();
        row1.put("prop1", "value1");
        row1.put("prop2", "value2");

        // k0
        List k0names = new ArrayList();
        k0names.add("prop3");

        Map k0row1 = new HashMap();
        k0row1.put("prop3", "value3a");
        Map k0row2 = new HashMap();
        k0row2.put("prop3", "value3b");

        List k0maps = new ArrayList();
        k0maps.add(k0row1);
        k0maps.add(k0row2);

        // k1
        List k1names = new ArrayList();
        k1names.add("prop4");
        k1names.add("prop5");

        Map k1row1 = new HashMap();
        k1row1.put("prop4", "value4");
        k1row1.put("prop5", "value5");

        List k1maps = new ArrayList();
        k1maps.add(k1row1);
        
        Answer k0 = new SimpleAnswer(k0maps.iterator(), k0names);
        Answer k1 = new SimpleAnswer(k1maps.iterator(), k1names);

        row1.put("k0", k0);
        row1.put("k1", k1);

        maps.add(row1);
        doTest(maps, names);


/* Answer { 
 *   Row {
 *     prop1 = value1
 *     prop2 = value2
 *     k0    = Answer {
 *                Row { prop3 = value3a }
 *                Row { prop3 = value3b }
 *             }
 *     k1    = Answer {
 *                Row { prop4 = value4a
                        prop5 = value5 }
 *             }
 *   }
 * }  
 */


    }

    private void doTest(List maps, List names) throws Exception {
        Answer original = new SimpleAnswer(maps.iterator(), names);
        if (m_printOriginal) {
            System.out.println("Original Answer");
            System.out.println("---------------");
            printAnswer(original);
            System.out.println();
        }
        Answer collapsed = new CollapsedAnswer(
                               new SimpleAnswer(maps.iterator(), names));
        if (m_printCollapsed) {
            System.out.println("Collapsed Answer");
            System.out.println("---------------");
            printAnswer(collapsed);
            System.out.println();
        }
    }

    private void printAnswer(Answer a) throws Exception {
        Variable[] vars = a.getVariables();
        System.out.print("Variables:");
        for (int i = 0; i < vars.length; i++) {
            System.out.print(" " + vars[i].getName());
        }
        System.out.println();
        a.beforeFirst();
        int row = 1;
        while (a.next()) {
            System.out.println("Row " + row + ":");
            row++;
            for (int i = 0; i < vars.length; i++) {
                String name = vars[i].getName();
                Object val = a.getObject(i);
                String valString;
                if (val == null) {
                    valString = "null";
                } else {
                    valString = "'" + val.toString() + "'";
                }
                System.out.println("    " + name + " = " + valString);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        CollapsedAnswerTest test = new CollapsedAnswerTest();
//        test.testNoRows();
//        test.testTwoRows();
        test.testComplexRows();
    }

}