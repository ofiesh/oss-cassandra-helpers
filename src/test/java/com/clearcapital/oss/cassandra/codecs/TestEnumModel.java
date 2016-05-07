package com.clearcapital.oss.cassandra.codecs;

import java.util.Arrays;
import java.util.Collection;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class TestEnumModel {

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("someEnums", someEnums).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final TestEnumModel that = (TestEnumModel) o;
        return Objects.equal(someEnums, that.someEnums);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(someEnums);
    }

    public enum TestEnum {
        FOO, BAR, FOOBAR
    }

    private Collection<TestEnum> someEnums;

    public TestEnumModel() {}

    public TestEnumModel(TestEnum... enums) {
        someEnums = Arrays.asList(enums);
    }
}
