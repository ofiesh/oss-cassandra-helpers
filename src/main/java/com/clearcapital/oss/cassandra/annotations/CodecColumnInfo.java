package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.java.patterns.NullClass;

public @interface CodecColumnInfo {

    /**
     * Codec to use for column values
     */
    Class<?> codecClass() default NullClass.class;

    /**
     * Codec to use for building up map entries
     */
    MapCodecInfo mapCodec() default @MapCodecInfo;
}
