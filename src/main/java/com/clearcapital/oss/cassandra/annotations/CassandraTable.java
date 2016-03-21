package com.clearcapital.oss.cassandra.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.clearcapital.oss.cassandra.annotations.table_properties.TableProperties;
import com.clearcapital.oss.cassandra.configuration.MultiRingConfiguration;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientMapper;
import com.clearcapital.oss.java.patterns.NullClass;

/**
 * Define object/cassandra mapping by annotating a class as being the interface to a table.
 * 
 * @author eehlinger
 *
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface CassandraTable {

    /**
     * The name of the table to create in Cassandra.
     * 
     * If left unspecified, the annotated table class will not support auto-schema.
     * 
     * Leave unspecified for a temporary table.
     */
    String tableName() default "";

    /**
     * The group identifier to use when going through the {@link MultiRingClientMapper} to determine which ring the
     * table should be in.
     * 
     * If left unspecified, the {@link MultiRingClientMapper} will use {@link MultiRingConfiguration#getDefaultRing} to
     * make that determination.
     */
    String multiRingGroup() default "";

    /**
     * The set of column definitions.
     * 
     * If set to {@link NullClass}.class, the annotated table class will not support writing or auto-schema.
     */
    // Class<?> columnDefinitions() default NullClass.class;
    Column[] columns() default {};

    /**
     * The Java class represented by each read Row.
     * 
     * If set to {@link NullClass}.class, the annotated table class will not support reading.
     */
    Class<?> modelClass() default NullClass.class;

    /**
     * 
     */
    TableProperties properties() default @TableProperties;

    boolean sortFields() default false;

    ClusteringOrder[] clusteringOrder() default {};

    AdditionalIndex[] additionalIndexes() default {};

    SolrOptions solrOptions() default @SolrOptions(enabled = false);
}
