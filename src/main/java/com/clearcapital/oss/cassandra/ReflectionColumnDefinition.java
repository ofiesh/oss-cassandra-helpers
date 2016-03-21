package com.clearcapital.oss.cassandra;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.UncheckedAssertHelpers;
import com.datastax.driver.core.DataType;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;


public class ReflectionColumnDefinition implements ColumnDefinition {

    String columnName;
    ImmutableList<String> javaPath;
    DataType cassandraDataType;
    ColumnOption columnOption;

    @Override
    public boolean getIsCreatedElsewhere() {
        return false;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public DataType getDataType() {
        return cassandraDataType;
    }

    @Override
    public ColumnOption getColumnOption() {
        return columnOption;
    }

    public ImmutableList<String> getJavaPath() {
        return javaPath;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("columnName", getColumnName())
                .add("columnOption", getColumnOption()).add("dataType", getDataType())
                .add("isCreatedElsewhere", getIsCreatedElsewhere()).add("javaPath", getJavaPath()).toString();
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {

        ReflectionColumnDefinition result = new ReflectionColumnDefinition();

        public ReflectionColumnDefinition build() throws IllegalStateException {
            UncheckedAssertHelpers.notNull(result.columnName, "result.cassandraDataType");
            UncheckedAssertHelpers.notNull(result.cassandraDataType, "result.cassandraDataType");
            UncheckedAssertHelpers.notNull(result.javaPath, "result.javaPath");
            return result;
        }

        public Builder fromAnnotation(Column columnAnnotation) {
            return setColumnName(columnAnnotation.cassandraName())
                    .setDataType(columnAnnotation.reflectionColumnInfo().dataType().getDataType())
                    .setJavaPath(ImmutableList.<String> copyOf(columnAnnotation.reflectionColumnInfo().javaPath()))
                    .setColumnOption(columnAnnotation.reflectionColumnInfo().columnOption());
        }

        public Builder setColumnName(String value) {
            result.columnName = value;
            return this;
        }

        public Builder setDataType(DataType value) {
            result.cassandraDataType = value;
            return this;
        }

        public Builder setJavaPath(ImmutableList<String> value) {
            result.javaPath = value;
            return this;
        }

        public Builder setColumnOption(ColumnOption value) {
            result.columnOption = value;
            return this;
        }
    }

}
