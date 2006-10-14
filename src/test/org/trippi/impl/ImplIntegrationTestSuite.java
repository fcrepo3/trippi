package org.trippi.impl;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.swingui.TestRunner;

public class ImplIntegrationTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(ImplIntegrationTestSuite.class.getName());
   
        // classes in this package
        //suite.addTestSuite(Whatever.class);

        // sub-package suites
//        suite.addTest(SomeIntegrationTestSuite.suite());

        return suite;

    }



    public static void main(String[] args) throws Exception {
        if (System.getProperty("text") != null && System.getProperty("text").equals("true")) {
            junit.textui.TestRunner.run(ImplIntegrationTestSuite.suite());
        } else {
            TestRunner.run(ImplIntegrationTestSuite.class);
        }
    }
}
