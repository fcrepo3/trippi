package org.trippi;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.swingui.TestRunner;

import org.trippi.impl.ImplUnitTestSuite;

public class TrippiUnitTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(TrippiUnitTestSuite.class.getName());
   
        // classes in this package
        //suite.addTestSuite(Whatever.class);

        // sub-package suites
        suite.addTest(ImplUnitTestSuite.suite());

        return suite;

    }

    public static void main(String[] args) throws Exception {
        if (System.getProperty("text") != null && System.getProperty("text").equals("true")) {
            junit.textui.TestRunner.run(TrippiUnitTestSuite.suite());
        } else {
            TestRunner.run(TrippiUnitTestSuite.class);
        }
    }
}
