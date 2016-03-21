package com.clearcapital.oss.cassandra.annotations;

/**
 * This is not a part of CassandraColumnDefinition because the clustering orders may be defined in a different order
 * from columns and therefore a different order from the clustering keys.
 * 
 * @author eehlinger
 *
 */
public @interface ClusteringOrder {

    /**
     * The name of the Cassandra column to use for clustering order.
     */
    String columnName();

    /**
     * If true, cluster in descending order. Otherwise, cluster in ascending order.
     */
    boolean descending() default false;
}
