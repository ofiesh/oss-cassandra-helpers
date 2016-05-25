package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.java.patterns.NullClass;

/**
 * Use this (only) when the column is meant to store a json representation of the entire record. 
 */
public @interface JsonColumnInfo {

    boolean isSelected() default true;

    Class<?> model() default NullClass.class;
}
