package com.clearcapital.oss.cassandra.annotations.table_properties;

import com.clearcapital.oss.cassandra.annotations.CassandraTable;

/**
 * Specify compression options inside {@link CassandraTable} annotation.
 * 
 * See also: <a href='http://docs.datastax.com/en/cql/3.3/cql/cql_reference/compressSubprop.html'>DSE docs</a>
 * 
 * @author eehlinger
 *
 */
public @interface Compression {

    static final int DEFAULT_CHUNK_LENGTH = -1;
    static final double DEFAULT_CHECK_CHANCE = -1;

    CompressionMethod sstableCompression() default CompressionMethod.DEFAULT;

    int chunkLengthKB() default DEFAULT_CHUNK_LENGTH;

    double crcCheckChance() default DEFAULT_CHECK_CHANCE;
}
