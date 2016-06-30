package com.clearcapital.oss.cassandra.codecs;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.SerializingException;

public interface CassandraCodec {

    public void encode(Map<String, Object> target, Object sourceObject) throws AssertException, SerializingException;

    public void decode(Object target, final Object fieldValue) throws AssertException, DeserializingException;

    public void initialize(Column annotation) throws AssertException;
}
