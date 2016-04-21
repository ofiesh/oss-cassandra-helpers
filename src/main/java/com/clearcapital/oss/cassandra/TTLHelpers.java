package com.clearcapital.oss.cassandra;

import java.util.concurrent.TimeUnit;

public class TTLHelpers {


    public static final String TTLColumnName = "ttl";

    public static Integer getTTL(final TimeUnit timeUnit, final Integer ttlDuration) {
        if (isValidTTL(timeUnit, ttlDuration)) {
            return Integer.valueOf((int) timeUnit.toSeconds(ttlDuration));
        }
        return null;
    }

    public static boolean isValidTTL(final Integer ttlDurationInSeconds) {
        if (ttlDurationInSeconds == null) {
            return false;
        }
        return isValidTTL(TimeUnit.SECONDS, ttlDurationInSeconds);
    }

    public static boolean isValidTTL(final TimeUnit timeUnit, final Integer ttlDuration) {
        if (timeUnit == null || ttlDuration == null) {
            return false;
        }
        if (timeUnit.toSeconds(ttlDuration) <= 0) {
            return false;
        }
        if (timeUnit.toSeconds(ttlDuration) > Integer.MAX_VALUE) {
            return false;
        }
        return true;
    }

    public static boolean areEqualTTL(final Integer ttlDuration1, final Integer ttlDuration2) {
        return areEqualTTL(TimeUnit.SECONDS, ttlDuration1, TimeUnit.SECONDS, ttlDuration2);
    }

    public static boolean areEqualTTL(final TimeUnit timeUnit1, final Integer ttlDuration1, final TimeUnit timeUnit2,
            final Integer ttlDuration2) {

        // If any one of them is not valid then we simply can't compare so return false
        if (!isValidTTL(timeUnit1, ttlDuration1) || !isValidTTL(timeUnit2, ttlDuration2)) {
            return false;
        }
        // Now both of them are valid TTL so compare seconds
        return timeUnit1.toSeconds(ttlDuration1) == timeUnit2.toSeconds(ttlDuration2);
    }

}
