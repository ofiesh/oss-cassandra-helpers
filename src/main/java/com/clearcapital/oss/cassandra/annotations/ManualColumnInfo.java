package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.cassandra.ColumnDefinition.ColumnOption;

public @interface ManualColumnInfo {

    boolean isSelected() default true;

    /**
     * Type of the field.
     */
    CassandraDataType dataType() default CassandraDataType.TEXT;

    /**
     * Additional options
     */
    ColumnOption columnOption() default ColumnOption.NULL;
}
