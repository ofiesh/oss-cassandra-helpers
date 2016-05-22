package com.clearcapital.oss.cassandra;

import static com.clearcapital.oss.cassandra.TTLHelpers.areEqualTTL;
import static com.clearcapital.oss.cassandra.TTLHelpers.getTTL;
import static com.clearcapital.oss.cassandra.TTLHelpers.isValidTTL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TTLHelpersTest {

    @Test
    public void testTTLMethods() {
        // Test isValidTTL Method
        assertFalse(isValidTTL(null, null));
        assertFalse(isValidTTL(null, 1));
        assertFalse(isValidTTL(TimeUnit.SECONDS, null));
        assertFalse(isValidTTL(TimeUnit.SECONDS, 0));
        assertFalse(isValidTTL(TimeUnit.SECONDS, -1));
        assertTrue(isValidTTL(TimeUnit.SECONDS, 1));
        assertTrue(isValidTTL(TimeUnit.SECONDS, Integer.MAX_VALUE));
        assertFalse(isValidTTL(TimeUnit.SECONDS, (Integer.MAX_VALUE) + 1));
        assertFalse(isValidTTL(TimeUnit.SECONDS, (Integer.MIN_VALUE)));

        assertFalse(isValidTTL(0));
        assertFalse(isValidTTL(-1));
        assertFalse(isValidTTL(Integer.MAX_VALUE + 1));
        assertTrue(isValidTTL(Integer.MAX_VALUE));
        assertTrue(isValidTTL(Integer.MAX_VALUE - 1));

        // Test areEqual method
        assertFalse(areEqualTTL(null, null, null, null));
        assertFalse(areEqualTTL(TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 1));
        assertFalse(areEqualTTL(TimeUnit.SECONDS, 1, TimeUnit.MILLISECONDS, 999));
        assertTrue(areEqualTTL(TimeUnit.SECONDS, 1, TimeUnit.MILLISECONDS, 1001));
        assertTrue(areEqualTTL(TimeUnit.MINUTES, 1, TimeUnit.SECONDS, 60));
        assertTrue(areEqualTTL(TimeUnit.MINUTES, 60, TimeUnit.HOURS, 1));

        assertFalse(areEqualTTL(null, null));
        assertFalse(areEqualTTL(1, 2));
        assertFalse(areEqualTTL(1, null));
        assertFalse(areEqualTTL(null, 1));
        assertTrue(areEqualTTL(1, 1));
        assertTrue(areEqualTTL(10, Integer.valueOf(10)));

        assertNull(getTTL(TimeUnit.SECONDS, null));
        assertNull(getTTL(TimeUnit.SECONDS, Integer.MAX_VALUE + 1));
        assertEquals(Integer.valueOf(10), getTTL(TimeUnit.SECONDS, 10));
        assertEquals(Integer.valueOf(600), getTTL(TimeUnit.MINUTES, 10));
        assertEquals(Integer.valueOf(3600), getTTL(TimeUnit.HOURS, 1));
        assertEquals(Integer.valueOf(3600 * 24), getTTL(TimeUnit.DAYS, 1));

    }

}
