package com.clearcapital.oss.cassandra.annotations;


public @interface AdditionalIndex {

    /**
     * Name of the index.
     */
    String name();

    /**
     * Column name to index.
     */
    String[] columnNames();

    /**
     * If true, indexes map keys from the map specified in {@link #columnName()}
     */
    boolean indexMapKeys() default false;

}
