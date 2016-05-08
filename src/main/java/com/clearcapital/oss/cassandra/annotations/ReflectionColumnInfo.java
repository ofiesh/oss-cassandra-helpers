package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.cassandra.ColumnDefinition.ColumnOption;

public @interface ReflectionColumnInfo {

    boolean isSelected() default true;

    CassandraDataType dataType() default CassandraDataType.TEXT;

    String[] javaPath() default {};

    ColumnOption columnOption() default ColumnOption.NULL;

}
