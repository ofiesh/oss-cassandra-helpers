package com.clearcapital.oss.cassandra.codecs;

import java.util.Map;

/**
 * Does nothing. Useful in the rare condition where one field is populated by another codec.
 */
public class CassandraColumnNullCodec implements CassandraColumnCodec {

    @Override
    public void encodeColumn(Map<String, Object> target, Object sourceObject) {
    }

    @Override
    public void decodeColumn(Object target, Object fieldValue) {
    }

}
