package com.clearcapital.oss.cassandra.annotations;

import com.clearcapital.oss.cassandra.codecs.CassandraCodec;

public @interface CodecColumnInfo {
    
    boolean isSelected() default true;

    /**
     * Codec to use for column values
     */
    Class<? extends CassandraCodec> codecClass() default CassandraCodec.class;

    /**
     * Type of the field.
     */
    CassandraDataType dataType() default CassandraDataType.TEXT;

    /**
     * Codec to use for building up map entries
     */
    MapCodecInfo mapCodec() default @MapCodecInfo;
}
