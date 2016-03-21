package com.clearcapital.oss.cassandra;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.datastax.driver.core.DataType;
import com.google.common.base.MoreObjects;

/**
 * It turns out that it can be quite useful to have Cassandra tables of the form: {id, json}.
 * 
 * In those cases, feel free to add a JsonColumnDefinition for the json column.
 */
public class JsonColumnDefinition implements ColumnDefinition {

    private String columnName;
    private Class<?> model;

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
        return DataType.text();
    }

    @Override
    public ColumnOption getColumnOption() {
        return ColumnOption.NULL;
    }

    public Class<?> getModel() {
        return model;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("columnName", getColumnName())
                .add("columnOption", getColumnOption()).add("dataType", getDataType())
                .add("isCreatedElsewhere", getIsCreatedElsewhere()).add("model", getModel()).toString();
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        JsonColumnDefinition result = new JsonColumnDefinition();

        public Builder setColumnName(String value) {
            result.columnName = value;
            return this;
        }

        public Builder setModel(Class<?> value) {
            result.model = value;
            return this;
        }

        public JsonColumnDefinition build() {
            return result;
        }

        public Builder fromAnnotation(Column column) {
            return setColumnName(column.cassandraName()).setModel(column.jsonColumnInfo().model());
        }
    }
}
