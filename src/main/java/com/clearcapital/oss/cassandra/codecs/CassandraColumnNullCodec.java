package com.clearcapital.oss.cassandra.codecs;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.exceptions.AssertException;

/**
 * Does nothing. Useful in the rare condition where one field is populated by another codec.
 */
public class CassandraColumnNullCodec implements CassandraCodec {

    @Override
    public void encode(Map<String, Object> target, Object sourceObject) {
    }

    @Override
    public void decode(Object target, Object fieldValue) {
    }

    @Override
    public void initialize(Column annotation) throws AssertException {
    }

}
