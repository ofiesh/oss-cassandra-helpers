package com.clearcapital.oss.cassandra.codecs;

import java.util.Arrays;
import java.util.Collection;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class TestSomeFields {

    private Collection<SomeField> someFields;

    public TestSomeFields() {}

    public TestSomeFields(SomeField... someFields) {
        this.someFields = Arrays.asList(someFields);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("someFields", someFields).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final TestSomeFields that = (TestSomeFields) o;
        return Objects.equal(someFields, that.someFields);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(someFields);
    }
}
