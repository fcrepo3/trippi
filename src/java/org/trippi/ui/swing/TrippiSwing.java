package org.trippi.ui.swing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.config.TrippiConfig;
import org.trippi.config.TrippiProfile;

public class TrippiSwing {

    private static final Logger logger =
        LoggerFactory.getLogger(TrippiSwing.class.getName());

    public TrippiSwing(TrippiConfig config, TrippiProfile profile) throws Exception {
        System.out.println("GUI implemented. Try -c");
    }

}
