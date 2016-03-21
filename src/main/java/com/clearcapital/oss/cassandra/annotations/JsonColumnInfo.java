package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.java.patterns.NullClass;

public @interface JsonColumnInfo {

    boolean isSelected() default true;

    Class<?> model() default NullClass.class;
}
