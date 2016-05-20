package com.clearcapital.oss.cassandra.annotations;


public @interface AdditionalIndex {

    /**
     * Name of the index.
     */
    String name();

    /**
     * Column name to index.
     */
    String columnName();
}
