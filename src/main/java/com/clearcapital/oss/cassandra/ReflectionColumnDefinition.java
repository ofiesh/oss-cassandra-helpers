package com.clearcapital.oss.cassandra;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import com.clearcapital.oss.cassandra.annotations.CassandraDataType;
import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.EnumHelpers;
import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.UncheckedAssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.clearcapital.oss.java.patterns.NullClass;
import com.clearcapital.oss.json.JsonSerializer;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ReflectionColumnDefinition implements ColumnDefinition {

    String columnName;
    ImmutableList<String> reflectionPath;
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

    public void decode(Object target, Row row, Definition column) throws AssertException, DeserializingException {
        try {
            Object value = RowHelpers.getColumn(row, column);
            if (value == null) {
                return;
            }
            Class<?> model = annotation.reflectionColumnInfo().model();
            if (annotation.reflectionColumnInfo().dataType().equals(CassandraDataType.JSON_TEXT)) {
                value = JsonSerializer.getInstance().getObject((String) value, model);
            } else {
                if (value instanceof Collection && (CollectionUtils.isEmpty((Collection<?>) value))) {
                    return;
                }
                if (value instanceof Map && (MapUtils.isEmpty((Map<?, ?>) value))) {
                    return;
                }
                if (value instanceof Collection && !model.equals(NullClass.class)) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> source = (Collection<Object>) value;
                    @SuppressWarnings("unchecked")
                    Collection<Object> replacement = source.getClass().newInstance();

                    if (Enum.class.isAssignableFrom(model)) {
                        source.forEach(entry -> replacement.add(EnumHelpers.getEnumValueUnchecked(model,(String)entry)));
                    } else {
                        source.forEach(entry -> replacement.add(JsonSerializer.getInstance().getObjectUnchecked((String)entry,model)));
                    }
                    value = replacement;
                }
            }
            ReflectionHelpers.setFieldValue(target, getReflectionPath(), value);
        } catch (AssertException | ReflectiveOperationException | RuntimeException e) {
            throw new DeserializingException(e);
        }
    }

    @Override
    public void encode(Map<String, Object> result, Object object) throws SerializingException {
        try {
            Object value = ReflectionHelpers.getFieldValue(object, getReflectionPath());
            if (annotation.reflectionColumnInfo().dataType().equals(CassandraDataType.JSON_TEXT)) {
                value = JsonSerializer.getInstance().getStringRepresentation(value);
            } else {
                if (value instanceof Enum) {
                    value = value.toString();
                } else if (value instanceof Set) {
                    Set<?> asSet = (Set<?>) value;
                    if (!asSet.isEmpty() && Iterables.get(asSet, 0) instanceof Enum) {
                        Set<Object> replacement = new LinkedHashSet<Object>();
                        asSet.forEach(entry -> replacement.add(entry.toString()));
                        value = replacement;
                    }
                }
            }
            result.put(getColumnName(), value);
        } catch (ReflectionPathException e) {
            throw new SerializingException(e);
        }
    }

    public ImmutableList<String> getReflectionPath() {
        return reflectionPath;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("columnName", getColumnName())
                .add("columnOption", getColumnOption()).add("dataType", getDataType())
                .add("isCreatedElsewhere", getIsCreatedElsewhere()).add("reflectionPath", getReflectionPath())
                .add("annotation", getAnnotation()).toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        ReflectionColumnDefinition result = new ReflectionColumnDefinition();

        public ReflectionColumnDefinition build() throws IllegalStateException {
            UncheckedAssertHelpers.notNull(result.columnName, "result.cassandraDataType");
            UncheckedAssertHelpers.notNull(result.cassandraDataType, "result.cassandraDataType");
            UncheckedAssertHelpers.notNull(result.reflectionPath, "result.javaPath");
            return result;
        }

        public Builder fromAnnotation(Column columnAnnotation) {
            return setAnnotation(columnAnnotation).setColumnName(columnAnnotation.cassandraName())
                    .setDataType(columnAnnotation.reflectionColumnInfo().dataType().getDataType())
                    .setReflectionPath(
                            ImmutableList.<String> copyOf(columnAnnotation.reflectionColumnInfo().javaPath()))
                    .setColumnOption(columnAnnotation.reflectionColumnInfo().columnOption());
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

        public Builder setReflectionPath(ImmutableList<String> value) {
            result.reflectionPath = value;
            return this;
        }

        public Builder setColumnOption(ColumnOption value) {
            result.columnOption = value;
            return this;
        }
    }

    @Override
    public boolean getIsIncludedInInsertStatement() {
        return true;
    }

}
