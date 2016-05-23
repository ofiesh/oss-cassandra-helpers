package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.java.patterns.NullClass;

public @interface MapCodecInfo {

    Class<?> keyClass() default NullClass.class;

    Class<?> valueClass() default NullClass.class;

    String prefixString() default "";

    String[] reflectionPath() default {};

    /**
     * If true, the codec will decode an empty map as "null" rather than as an empty map.
     * @return
     */
    boolean nullifyEmptyMap() default false;

}
