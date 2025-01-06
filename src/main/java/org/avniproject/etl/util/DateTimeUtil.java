package org.avniproject.etl.util;

import java.util.Date;

public class DateTimeUtil {
    public static Date nowPlusSeconds(int numberOfSeconds) {
        Date startTime = new Date();
        return new Date(startTime.getTime() + numberOfSeconds * 1000L);
    }
    public static Date nowMinusMinutes(int numberOfMinutes) {
        Date startTime = new Date();
        return new Date(startTime.getTime() - numberOfMinutes * 60 * 1000L);
    }
}
