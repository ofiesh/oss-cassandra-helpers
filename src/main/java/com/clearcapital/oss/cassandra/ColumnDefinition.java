package com.clearcapital.oss.cassandra;

import com.datastax.driver.core.DataType;

public interface ColumnDefinition {

    public static enum ColumnOption {
        NULL,
        STATIC,
        PARTITION_KEY,
        CLUSTERING_KEY
    }

    /**
     * If true, this definition should not be created during a "CREATE TABLE" CQL statement; it is created by Cassandra
     * itself, or by integrations thereof, like DSE/Solr.
     */
    public boolean getIsCreatedElsewhere();

    /**
     * Name of the column, from Cassandra's perspective.
     */
    public String getColumnName();

    /**
     * Data type of the column, from Cassandra's perspective.
     */
    public DataType getDataType();

    /**
     * Determines whether the column is static, a part of the partition key, or of the clustering key.
     * 
     * If {@link ColumnOption#NULL}, the column is none of the above (just a regular column)
     * 
     * No implementation should return NULL.
     */
    public ColumnOption getColumnOption();

}
