package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.java.patterns.NullClass;

public @interface MapCodecInfo {

    Class<?> keyClass() default NullClass.class;

    Class<?> valueClass() default NullClass.class;

    String prefixString() default "";

    String[] reflectionPath() default {};

}
