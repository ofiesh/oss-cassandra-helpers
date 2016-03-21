package com.clearcapital.oss.cassandra.annotations.table_properties;

import com.datastax.driver.core.schemabuilder.SchemaBuilder.KeyCaching;

/**
 * 
 * See also: <a href='http://docs.datastax.com/en/cql/3.3/cql/cql_reference/compressSubprop.html'>DSE docs</a>
 * 
 * @author eehlinger
 */
public @interface Caching {

    public static final int ROWS_UNSPECIFIED = Integer.MIN_VALUE;
    public static final int ROWS_ALL = Integer.MIN_VALUE + 1;
    public static final int ROWS_NONE = 0;

    boolean useDefault() default true;

    KeyCaching keys() default KeyCaching.ALL;
    int rowsPerPartition() default ROWS_UNSPECIFIED;

}
