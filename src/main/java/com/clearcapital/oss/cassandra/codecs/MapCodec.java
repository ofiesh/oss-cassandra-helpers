package com.clearcapital.oss.cassandra.codecs;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.exceptions.DeserializingException;

public class MapCodec<KEY_TYPE, VALUE_TYPE> implements CassandraColumnCodec {

    private final String prefix;
    private final Class<?> keyType;
    private Collection<String> reflectionPath;
    private String cassandraColumnName;

    public MapCodec(String prefix, Class<?> keyType) {
        this.prefix = prefix;
        this.keyType = keyType;
    }

    @Override
    public void encodeColumn(Map<String, Object> target, Object sourceObject) throws Exception {
        Map<String, VALUE_TYPE> encodedMap = null;
        Object rawFieldValue = ReflectionHelpers.getFieldValue(sourceObject, reflectionPath);
        if (rawFieldValue != null) {
            @SuppressWarnings("unchecked")
            Map<?, VALUE_TYPE> decodedMap = (Map<?, VALUE_TYPE>) rawFieldValue;
            encodedMap = encode(decodedMap, prefix);
        }
        target.put(cassandraColumnName, encodedMap);
    }

    @Override
    public void decodeColumn(Object target, Object fieldValue) throws Exception {
        if (fieldValue == null) {
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, VALUE_TYPE> encodedMap = (Map<String, VALUE_TYPE>) fieldValue;
        Map<KEY_TYPE, VALUE_TYPE> decodedMap = decode(encodedMap, prefix, keyType);
        ReflectionHelpers.setFieldValue(target, reflectionPath, decodedMap);
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
    public static <K> K decodeKey(String decodedKeyString, Type type) throws Exception {
        if (type == String.class) {
            return (K) decodedKeyString;
        }
        if (type == Long.class) {
            return (K) Long.valueOf(decodedKeyString);
        }
        throw new DeserializingException("Unrecognized target key type", null);
    }

    public static <K, V> Map<K, V> decode(Map<String, V> map, String prefix, Class<?> keyType) throws Exception {
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
