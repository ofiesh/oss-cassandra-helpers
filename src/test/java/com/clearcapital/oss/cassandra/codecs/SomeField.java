package com.clearcapital.oss.cassandra.codecs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class SomeField {
    @JsonProperty
    private String foo;
    @JsonProperty
    private String bar;

    public SomeField() {}

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final SomeField someField = (SomeField) o;
        return Objects.equal(foo, someField.foo) && Objects.equal(bar, someField.bar);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(foo, bar);
    }

    public SomeField(String foo, String bar) {
        this.foo = foo;
        this.bar = bar;

    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("foo", foo).add("bar", bar).toString();
    }
}
