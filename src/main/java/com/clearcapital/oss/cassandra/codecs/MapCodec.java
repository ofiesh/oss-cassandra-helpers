package com.clearcapital.oss.cassandra.codecs;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.Response.Status;

import com.clearcapital.core.util.exceptions.CoreException;
import com.clearcapital.core.util.exceptions.CoreExceptionFactory;

public class MapCodec<KEY_TYPE, VALUE_TYPE> implements CassandraColumnCodec {

    private final String prefix;
    private final Class<?> keyType;

    public MapCodec(String prefix, Class<?> keyType) {
        this.prefix = prefix;
        this.keyType = keyType;
    }

    @Override
    public void encodeColumn(Map<String, Object> target, Object sourceObject, CassandraColumnDefinition columnDefinition)
            throws CoreException {
        Map<String, VALUE_TYPE> encodedMap = null;
        Object rawFieldValue = CassandraTableProcessor.getFieldValue(sourceObject, columnDefinition);
        if (rawFieldValue != null) {
            @SuppressWarnings("unchecked")
            Map<?, VALUE_TYPE> decodedMap = (Map<?, VALUE_TYPE>) rawFieldValue;
            encodedMap = encode(decodedMap, prefix);
        }
        target.put(columnDefinition.getCassandraName(), encodedMap);
    }

    @Override
    public void decodeColumn(Object target, Object fieldValue, CassandraColumnDefinition columnDefinition)
            throws CoreException {
        if (fieldValue == null) {
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, VALUE_TYPE> encodedMap = (Map<String, VALUE_TYPE>) fieldValue;
        Map<KEY_TYPE, VALUE_TYPE> decodedMap = decode(encodedMap, prefix, keyType);
        CassandraTableProcessor.setFieldValue(target, decodedMap, columnDefinition);
    }

    public static <VALUE_TYPE> Map<String, VALUE_TYPE> encode(Map<?, VALUE_TYPE> rawObject, String prefix) {
        Map<?, VALUE_TYPE> sourceMap = rawObject;
        Map<String, VALUE_TYPE> encodedMap = null;
        if (sourceMap != null) {
            encodedMap = new HashMap<>();
            for (Entry<?, VALUE_TYPE> entry : sourceMap.entrySet()) {
                encodedMap.put(prefix + entry.getKey(), entry.getValue());
            }
        }
        return encodedMap;
    }

    @SuppressWarnings("unchecked")
    public static <K> K decodeKey(String decodedKeyString, Type type) throws CoreException {
        if (type == String.class) {
            return (K) decodedKeyString;
        }
        if (type == Long.class) {
            return (K) Long.valueOf(decodedKeyString);
        }
        throw CoreExceptionFactory.createException("Unrecognized target key type", Status.INTERNAL_SERVER_ERROR);
    }

    public static <K, V> Map<K, V> decode(Map<String, V> map, String prefix, Class<?> keyType) throws CoreException {
        if (map == null) {
            return null;
        }
        Map<K, V> decodedMap = new HashMap<>();
        for (Entry<String, V> entry : map.entrySet()) {
            String keyStr = entry.getKey();
            String decodedKeyString = keyStr.substring(prefix.length());

            K decodedKey = decodeKey(decodedKeyString, keyType);

            decodedMap.put(decodedKey, entry.getValue());
        }
        return decodedMap;
    }
}
