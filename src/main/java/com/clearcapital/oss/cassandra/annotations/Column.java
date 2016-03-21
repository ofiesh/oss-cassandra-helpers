package com.clearcapital.oss.cassandra.annotations;

/**
 * <p>
 * Define a column.
 * </p>
 * 
 * {@link #cassandraName} determines, shockingly, what the name of the column will be in Cassandra.
 * 
 * <p>
 * <b>Specify a codec for this column by setting one of the following:</b>
 * </p>
 *
 * <ul>
 * <li>{@link reflectionColumnInfo} - use Reflection to get/set the specified member.</li>
 * <li>{@link jsonColumnInfo} - use JSON to codec the entire model object</li>
 * </ul>
 * 
 * @author eehlinger
 */
public @interface Column {

    String cassandraName();

    boolean createdElsewhere() default false;

    ReflectionColumnInfo reflectionColumnInfo() default @ReflectionColumnInfo(isSelected = false);

    JsonColumnInfo jsonColumnInfo() default @JsonColumnInfo(isSelected = false);

}
