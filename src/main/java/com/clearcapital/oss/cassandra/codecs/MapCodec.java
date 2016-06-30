package com.clearcapital.oss.cassandra.codecs;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;

public class MapCodec<KEY_TYPE, VALUE_TYPE> implements CassandraCodec {

    private String prefix;
    private Class<?> keyType;
    private Collection<String> reflectionPath;
    private String cassandraColumnName;
    private boolean nullifyEmptyMap;

    public MapCodec() {
    }
    
    @Override
    public void initialize(Column annotation) throws AssertException {
        this.cassandraColumnName = annotation.cassandraName();
        this.reflectionPath = Arrays.asList(annotation.codecColumnInfo().mapCodec().reflectionPath());
        this.prefix = annotation.codecColumnInfo().mapCodec().prefixString();
        this.keyType = annotation.codecColumnInfo().mapCodec().keyClass();
        this.nullifyEmptyMap = annotation.codecColumnInfo().mapCodec().nullifyEmptyMap();
    }

    @Override
    public void encode(Map<String, Object> target, Object sourceObject) throws AssertException, SerializingException {
        try {
            Map<String, VALUE_TYPE> encodedMap = null;
            Object rawFieldValue = ReflectionHelpers.getFieldValue(sourceObject, reflectionPath);
            if (rawFieldValue != null) {
                @SuppressWarnings("unchecked")
                Map<?, VALUE_TYPE> decodedMap = (Map<?, VALUE_TYPE>) rawFieldValue;
                encodedMap = encode(decodedMap, prefix);
            }
            target.put(cassandraColumnName, encodedMap);
        } catch (ReflectionPathException e) {
            throw new SerializingException(e);
        }
    }

    @Override
    public void decode(Object target, Object fieldValue) throws AssertException, DeserializingException {
        try {
            if (fieldValue == null) {
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, VALUE_TYPE> encodedMap = (Map<String, VALUE_TYPE>) fieldValue;
            Map<KEY_TYPE, VALUE_TYPE> decodedMap = decode(encodedMap, prefix, keyType);
            if (nullifyEmptyMap && decodedMap.isEmpty()) {
                decodedMap = null;
            }
            ReflectionHelpers.setFieldValue(target, reflectionPath, decodedMap);
        } catch (ReflectiveOperationException e) {
            throw new DeserializingException(e);
        }
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
    public static <K> K decodeKey(String decodedKeyString, Type type) throws DeserializingException {
        if (type == String.class) {
            return (K) decodedKeyString;
        }
        if (type == Long.class) {
            return (K) Long.valueOf(decodedKeyString);
        }
        throw new DeserializingException("Unrecognized target key type", null);
    }

    public static <K, V> Map<K, V> decode(Map<String, V> map, String prefix, Class<?> keyType)
            throws DeserializingException {
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
