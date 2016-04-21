package com.clearcapital.oss.cassandra;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.datastax.driver.core.DataType;

public interface ColumnDefinition {

    public static enum ColumnOption {
        NULL,
        STATIC,
        PARTITION_KEY,
        CLUSTERING_KEY
    }

    /**
     * The annotation that this definition was derived from.
     */
    public Column getAnnotation();    
    
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

    /**
     * Encode relevant portion(s) of object as an entry in result.
     */
	public void encode(Map<String, Object> result, Object object) throws SerializingException, ReflectionPathException;

}
