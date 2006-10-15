package org.trippi.impl;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.swingui.TestRunner;

import org.trippi.TestConfig;
import org.trippi.config.TrippiProfile;
import org.trippi.impl.mpt.MPTIntegrationTestSuite;

public class ImplIntegrationTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(ImplIntegrationTestSuite.class.getName());
   
        // classes in this package
        //suite.addTestSuite(Whatever.class);

        // sub-package suites

        // add sub-package suite based on current test configuration
        TrippiProfile profile = TestConfig.getTestProfile();
        String name = profile.getConnectorClassName();
        if (name.equals("org.trippi.impl.mpt.MPTConnector")) {
            suite.addTest(MPTIntegrationTestSuite.suite());
        } else {
            throw new RuntimeException("Don't know integration test suite for "
                  + "connector: " + name);
        }

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
