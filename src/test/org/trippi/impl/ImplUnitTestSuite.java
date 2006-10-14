package org.trippi.impl;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.swingui.TestRunner;

import org.trippi.impl.base.BaseUnitTestSuite;

public class ImplUnitTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(ImplUnitTestSuite.class.getName());
   
        // classes in this package
        //suite.addTestSuite(Whatever.class);

        // sub-package suites
        suite.addTest(BaseUnitTestSuite.suite());

        return suite;

    }

    public static void main(String[] args) throws Exception {
        if (System.getProperty("text") != null && System.getProperty("text").equals("true")) {
            junit.textui.TestRunner.run(ImplUnitTestSuite.suite());
        } else {
            TestRunner.run(ImplUnitTestSuite.class);
        }
    }
}
