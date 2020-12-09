package com.fanpan26.akfak.common.utils;

/**
 * @author fanyuepan
 */
public interface Time {

    /**
     * The current time in milliseconds
     */
    long milliseconds();

    /**
     * The current time in nanoseconds
     */
    long nanoseconds();

    /**
     * Sleep for the given number of milliseconds
     */
    void sleep(long ms);

}
