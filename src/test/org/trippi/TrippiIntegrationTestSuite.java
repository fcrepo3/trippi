package org.trippi;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.swingui.TestRunner;

import org.trippi.impl.ImplIntegrationTestSuite;

public class TrippiIntegrationTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(TrippiIntegrationTestSuite.class.getName());
   
        // classes in this package
        //suite.addTestSuite(Whatever.class);

        // sub-package suites
        suite.addTest(ImplIntegrationTestSuite.suite());

        return suite;

    }



    public static void main(String[] args) throws Exception {
        if (System.getProperty("text") != null && System.getProperty("text").equals("true")) {
            junit.textui.TestRunner.run(TrippiIntegrationTestSuite.suite());
        } else {
            TestRunner.run(TrippiIntegrationTestSuite.class);
        }
    }
}
