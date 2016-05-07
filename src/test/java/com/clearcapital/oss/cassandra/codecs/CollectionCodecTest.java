package com.clearcapital.oss.cassandra.codecs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CollectionCodecTest {

    @Test
    public void testEncodeObjectModelCollection() throws Exception {
        CollectionCodec codec = CollectionCodec.builder()
                .setReflectionPath(ImmutableList.of("someFields"))
                .setCassandraColumnName("some_fields")
                .setModelClass(SomeField.class)
                .build();

        Map<String, Object> actual = new HashMap<>();

        codec.encodeColumn(actual, new TestSomeFields(new SomeField("a", "b"), new SomeField("b", "c")));

        ImmutableMap<String, List<String>> expected = ImmutableMap.of("some_fields",
                Arrays.asList("{\"foo\":\"a\",\"bar\":\"b\"}", "{\"foo\":\"b\",\"bar\":\"c\"}"));

        Assert.assertEquals("List of enums did not encode someFields=[{\"foo\":\"a\",\"bar\":\"b\"},"
                + "{\"foo\":\"b\",\"bar\":\"c\"}]", expected, actual);
    }

    @Test
    public void testDecodeObjectModelCollection() throws Exception {
        CollectionCodec codec = CollectionCodec.builder()
                .setReflectionPath(ImmutableList.of("someFields"))
                .setCassandraColumnName("some_fields")
                .setModelClass(SomeField.class)
                .build();

        TestSomeFields actual = new TestSomeFields();

        codec.decodeColumn(actual, Arrays.asList("{\"foo\":\"a\",\"bar\":\"b\"}", "{\"foo\":\"b\",\"bar\":\"c\"}"));

        Assert.assertEquals(new TestSomeFields(new SomeField("a", "b"), new SomeField("b", "c")), actual);
    }
}
