package com.clearcapital.oss.cassandra.codecs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.clearcapital.core.util.exceptions.CoreException;
import com.clearcapital.datastax.annotations.SomeField;
import com.clearcapital.datastax.annotations.TestEnumModel;
import com.clearcapital.datastax.annotations.TestSomeFields;
import com.clearcapital.datastax.annotations.TestEnumModel.TestEnum;
import com.clearcapital.oss.cassandra.codecs.CollectionCodec;
import com.google.common.collect.ImmutableMap;

public class CollectionCodecTest {

    @Test
    public void testEncodeEnumList() throws CoreException {
        CollectionCodec codec = new CollectionCodec();

        Map<String, Object> actual = new HashMap<>();

        codec.encodeColumn(actual, new TestEnumModel(TestEnum.FOO, TestEnum.FOOBAR),
                new CassandraColumnDefinition("someEnums", "some_enums",
                        CassandraColumnDefinition.CassandraDataType.LIST_TEXT, TestEnum.class));

        Assert.assertEquals("List of enums did not encode some_enums=[BAR, FOOBAR]",
                ImmutableMap.of("some_enums", Arrays.asList("FOO", "FOOBAR")), actual);
    }

    @Test
    public void testDecodeEnumList() throws CoreException {
        CollectionCodec codec = new CollectionCodec();

        TestEnumModel actual = new TestEnumModel();

        codec.decodeColumn(actual, Arrays.asList("BAR", "FOOBAR"),
                new CassandraColumnDefinition("someEnums", "some_enums",
                        CassandraColumnDefinition.CassandraDataType.LIST_TEXT, TestEnum.class));

        Assert.assertEquals("List of enums did not decode to someEnums=[TestEnum.BAR, TestEnum.FOOBAR]",
               new TestEnumModel(TestEnum.BAR, TestEnum.FOOBAR), actual);
    }



    @Test
    public void testEncodeObjectModelCollection() throws CoreException {
        CollectionCodec codec = new CollectionCodec();

        Map<String, Object> actual = new HashMap<>();

        codec.encodeColumn(actual, new TestSomeFields(new SomeField("a", "b"), new SomeField("b", "c")),
                new CassandraColumnDefinition("someFields", "some_fields",
                        CassandraColumnDefinition.CassandraDataType.LIST_TEXT, TestEnum.class));

        ImmutableMap<String, List<String>> expected = ImmutableMap.of("some_fields",
                Arrays.asList("{\"foo\":\"a\",\"bar\":\"b\"}", "{\"foo\":\"b\",\"bar\":\"c\"}"));

        Assert.assertEquals("List of enums did not encode someFields=[{\"foo\":\"a\",\"bar\":\"b\"},"
                + "{\"foo\":\"b\",\"bar\":\"c\"}]", expected, actual);
    }

    @Test
    public void testDecodeObjectModelCollection() throws CoreException {
        CollectionCodec codec = new CollectionCodec();

        TestSomeFields actual = new TestSomeFields();

        codec.decodeColumn(actual, Arrays.asList("{\"foo\":\"a\",\"bar\":\"b\"}", "{\"foo\":\"b\",\"bar\":\"c\"}"),
                new CassandraColumnDefinition("someFields", "som_fields",
                        CassandraColumnDefinition.CassandraDataType.LIST_TEXT, SomeField.class));

        Assert.assertEquals(new TestSomeFields(new SomeField("a", "b"), new SomeField("b", "c")), actual);
    }
}
