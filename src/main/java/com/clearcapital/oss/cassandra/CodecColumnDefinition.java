package com.clearcapital.oss.cassandra;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.cassandra.codecs.CassandraCodec;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.base.MoreObjects;

/**
 * A definition for a column that is codec'd by a custom class.
 */
public class CodecColumnDefinition implements ColumnDefinition {

    private String columnName;
    private Column annotation;
    public Class<? extends CassandraCodec> codecClass;
    public CassandraCodec codec;

    @Override
    public Column getAnnotation() {
        return annotation;
    }
    
    public CassandraCodec getCodec() {
        return codec;
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
        return annotation.codecColumnInfo().dataType().getDataType();
    }

    @Override
    public ColumnOption getColumnOption() {
        return ColumnOption.NULL;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("columnName", getColumnName())
                .add("isCreatedElsewhere", getIsCreatedElsewhere()).add("annotation", getAnnotation()).toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        CodecColumnDefinition result = new CodecColumnDefinition();

        public Builder setColumnName(String value) {
            result.columnName = value;
            return this;
        }

        public CodecColumnDefinition build() throws AssertException {
            AssertHelpers.notNull(result.codecClass, "result.codecClass");
            AssertHelpers.notNull(result.columnName, "result.columnName");
            
            try {
                CassandraCodec codec = (CassandraCodec)result.codecClass.newInstance();
                codec.initialize(result.annotation);
                result.codec = codec;
                return result;
            } catch (InstantiationException | IllegalAccessException e) {
                throw new AssertException("column " + result.columnName + " - could not instantiate codecClass:" + result.codecClass.getName(), e);
            }
        }

        public Builder setAnnotation(Column value) {
            result.annotation = value;
            return this;
        }

        public Builder fromAnnotation(Column column) {
            return setAnnotation(column).setColumnName(column.cassandraName())
                    .setCodecClass(column.codecColumnInfo().codecClass());
        }

        private Builder setCodecClass(Class<? extends CassandraCodec> codecClass) {
            result.codecClass = codecClass;
            return this;
        }
    }

    @Override
    public void encode(Map<String, Object> result, Object object) throws AssertException, SerializingException {
        getCodec().encode(result, object);
    }

    @Override
    public boolean getIsIncludedInInsertStatement() {
        return true;
    }

    @Override
    public void decode(Object target, Row row, Definition column) throws AssertException, DeserializingException {
        Object fieldValue = CQLHelpers.getColumn(row, column);
        getCodec().decode(target, fieldValue);
    }

}
