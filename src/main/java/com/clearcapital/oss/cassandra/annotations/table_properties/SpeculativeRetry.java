package com.clearcapital.oss.cassandra.annotations.table_properties;

/**
 * 
 * See also: <a href='http://docs.datastax.com/en/cql/3.3/cql/cql_reference/compressSubprop.html'>DSE docs</a>
 * 
 * @author eehlinger
 */
public @interface SpeculativeRetry {

    public static enum When {
        CHOOSE_PERCENTILE_THEN_MS,
        ALWAYS,
        NONE
    };

    public static final int PERCENTILE_USE_MS = Integer.MIN_VALUE;

    boolean useDefault() default false;

    When when() default When.CHOOSE_PERCENTILE_THEN_MS;

    int percentile() default PERCENTILE_USE_MS;

    int milliseconds() default 10;
}
