package com.clearcapital.oss.cassandra.codecs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.clearcapital.oss.cassandra.codecs.TestEnumModel.TestEnum;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CollectionEnumCodecTest {

    @Test
    public void testEncodeEnumList() throws Exception {
        CollectionEnumCodec<TestEnum> codec = CollectionEnumCodec.<TestEnum> builder()
                .setReflectionPath(ImmutableList.of("someEnums")).setCassandraColumnName("some_enums")
                .setModelClass(TestEnum.class).build();

        Map<String, Object> actual = new HashMap<>();

        codec.encode(actual, new TestEnumModel(TestEnum.FOO, TestEnum.FOOBAR));

        Assert.assertEquals("List of enums did not encode some_enums=[BAR, FOOBAR]",
                ImmutableMap.of("some_enums", Arrays.asList("FOO", "FOOBAR")), actual);
    }

    @Test
    public void testDecodeEnumList() throws Exception {
        CollectionEnumCodec<TestEnum> codec = CollectionEnumCodec.<TestEnum> builder()
                .setReflectionPath(ImmutableList.of("someEnums")).setCassandraColumnName("some_enums")
                .setModelClass(TestEnum.class).build();

        TestEnumModel actual = new TestEnumModel();

        codec.decode(actual, Arrays.asList("BAR", "FOOBAR"));

        Assert.assertEquals("List of enums did not decode to someEnums=[TestEnum.BAR, TestEnum.FOOBAR]",
                new TestEnumModel(TestEnum.BAR, TestEnum.FOOBAR), actual);
    }
}
