package org.trippi;

/**
 * A Trippi-related exception.
 *
 * @author cwilper@cs.cornell.edu
 */
public class TrippiException extends Exception {
    private static final long serialVersionUID = 1L;

    public TrippiException(String message) {
        super(message);
    }

    public TrippiException(String message, Throwable cause) {
        super(message, cause);
    }

}