package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.cassandra.ColumnDefinition.ColumnOption;
import com.clearcapital.oss.java.patterns.NullClass;

public @interface ReflectionColumnInfo {

    boolean isSelected() default true;

    CassandraDataType dataType() default CassandraDataType.TEXT;

    String[] javaPath() default {};

    ColumnOption columnOption() default ColumnOption.NULL;

    /**
     * <p>
     * Use this when {@link #dataType} is a collection of TEXT, or {@link CassandraDataType#JSON_TEXT} to indicate that
     * the data should be stored in Cassandra as text, but as a different type of Java object.
     * </p>
     */
    Class<?> model() default NullClass.class;
}
