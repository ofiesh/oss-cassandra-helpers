package com.clearcapital.oss.cassandra;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.UncheckedAssertHelpers;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.base.MoreObjects;

public class SolrCopyFieldDefinition implements ColumnDefinition {

    String columnName;
    DataType cassandraDataType;
    ColumnOption columnOption;
    Column annotation;

    @Override
    public Column getAnnotation() {
        return annotation;
    }

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("columnName", getColumnName())
                .add("columnOption", getColumnOption()).add("dataType", getDataType())
                .add("isCreatedElsewhere", getIsCreatedElsewhere())
                .add("annotation", getAnnotation()).toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        SolrCopyFieldDefinition result = new SolrCopyFieldDefinition();

        public SolrCopyFieldDefinition build() throws IllegalStateException {
            UncheckedAssertHelpers.notNull(result.columnName, "result.cassandraDataType");
            UncheckedAssertHelpers.notNull(result.cassandraDataType, "result.cassandraDataType");
            return result;
        }

        public Builder fromAnnotation(Column columnAnnotation) {
            return setAnnotation(columnAnnotation).setColumnName(columnAnnotation.cassandraName())
                    .setDataType(columnAnnotation.solrCopyField().dataType().getDataType())
                    .setColumnOption(ColumnOption.NULL);
        }

        public Builder setAnnotation(Column value) {
            result.annotation = value;
            return this;
        }

        public Builder setColumnName(String value) {
            result.columnName = value;
            return this;
        }

        public Builder setDataType(DataType value) {
            result.cassandraDataType = value;
            return this;
        }

        public Builder setColumnOption(ColumnOption value) {
            result.columnOption = value;
            return this;
        }
    }

    @Override
    public void encode(Map<String, Object> result, Object object) {
        // NO-OP. This column will be populated (maybe?) by the table class.
    }

    @Override
    public boolean getIsIncludedInInsertStatement() {
        return false;
    }

    @Override
    public void decode(Object target, Row row, Definition column) {
        // NO-OP. This column will be populated (maybe?) by the table class.
    }
}
