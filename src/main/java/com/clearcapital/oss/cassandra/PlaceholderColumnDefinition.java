package com.clearcapital.oss.cassandra;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.datastax.driver.core.DataType;
import com.google.common.base.MoreObjects;

/**
 * It turns out that it can be quite useful to have Cassandra tables of the form: {id, json}.
 * 
 * In those cases, feel free to add a JsonColumnDefinition for the json column.
 */
public class PlaceholderColumnDefinition implements ColumnDefinition {

    private String columnName;

    @Override
    public boolean getIsCreatedElsewhere() {
        return true;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public DataType getDataType() {
        return null;
    }

    @Override
    public ColumnOption getColumnOption() {
        return ColumnOption.NULL;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("columnName", getColumnName())
                .add("isCreatedElsewhere", getIsCreatedElsewhere()).toString();
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        PlaceholderColumnDefinition result = new PlaceholderColumnDefinition();

        public Builder setColumnName(String value) {
            result.columnName = value;
            return this;
        }

        public PlaceholderColumnDefinition build() {
            return result;
        }

        public Builder fromAnnotation(Column column) {
            return setColumnName(column.cassandraName());
        }
    }
}
