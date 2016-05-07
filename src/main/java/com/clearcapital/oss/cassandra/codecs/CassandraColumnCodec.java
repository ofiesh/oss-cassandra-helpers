package com.clearcapital.oss.cassandra.codecs;

import java.util.Map;

public interface CassandraColumnCodec {

    public void encodeColumn(Map<String, Object> target, Object sourceObject) throws Exception;

    public void decodeColumn(Object target, final Object fieldValue) throws Exception;
}
