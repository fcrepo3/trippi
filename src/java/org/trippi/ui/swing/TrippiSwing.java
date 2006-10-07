package org.trippi.ui.swing;

import org.apache.log4j.Logger;
import org.trippi.config.TrippiConfig;
import org.trippi.config.TrippiProfile;

public class TrippiSwing {

    private static final Logger logger =
        Logger.getLogger(TrippiSwing.class.getName());

    public TrippiSwing(TrippiConfig config, TrippiProfile profile) throws Exception {
        System.out.println("GUI implemented. Try -c");
    }

}
