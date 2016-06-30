package com.clearcapital.oss.cassandra.codecs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;

/*
 * A codec that can serialize and deserialize between a cql type {@code set<text>} and a Java type
 * {@link Collection<?>}. The Java type is determined by the {@link CassandraColumnDefinition} {@code jsonType}.
 *
 * {@code CollectionCodec} should never used for {@link Collection<String>}.
 *
 * {@code CollectionCodec} is thread safe.
 */
public class CollectionEnumCodec<T extends Enum<T>> implements CassandraCodec {

    Collection<String> reflectionPath;
    String cassandraColumnName;
    Class<T> modelClass;

    public static <T extends Enum<T>> Builder<T> builder() {
        return new Builder<T>(new CollectionEnumCodec<T>());
    }

    Class<?> getCollectionClass() {
        Class<?> result = ArrayList.class;
        return result;
    }

    public static class Builder<T extends Enum<T>> {

        private CollectionEnumCodec<T> result;

        Builder(CollectionEnumCodec<T> result) {
            this.result = result;
        }

        public Builder<T> setReflectionPath(Collection<String> value) {
            result.reflectionPath = value;
            return this;
        }

        public Builder<T> setCassandraColumnName(String value) {
            result.cassandraColumnName = value;
            return this;
        }

        public Builder<T> setModelClass(Class<T> value) {
            result.modelClass = value;
            return this;
        }

        public CollectionEnumCodec<T> build() throws AssertException {
            AssertHelpers.notNull(result, "result");
            AssertHelpers.notNull(result.modelClass, "result.modelClass");
            AssertHelpers.notNull(result.reflectionPath, "result.reflectionPath");
            AssertHelpers.notNull(result.cassandraColumnName, "result.cassandraColumnName");
            return result;
        }
    }

    @Override
    public void encode(final Map<String, Object> target, final Object sourceObject)
            throws AssertException, SerializingException {
        AssertHelpers.notNull(target, "target");
        AssertHelpers.notNull(sourceObject, "sourceObject");

        try {
            Object fieldValue = ReflectionHelpers.getFieldValue(sourceObject, reflectionPath);

            if (fieldValue != null) {
                @SuppressWarnings("unchecked")
                Collection<Object> collection = (Collection<Object>) getCollectionClass().newInstance();
                AssertHelpers.isTrue(fieldValue instanceof Collection, "Cannot use collection codec on non set field");
                if (fieldValue instanceof Collection) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> fieldSet = (Collection<Object>) fieldValue;

                    for (Object item : fieldSet) {
                        collection.add(item.toString());
                    }
                }
                target.put(cassandraColumnName, collection);
            } else {
                target.put(cassandraColumnName, null);
            }
        } catch (ReflectionPathException | InstantiationException | IllegalAccessException e) {
            throw new SerializingException(e);
        }
    }

    @Override
    public void decode(final Object target, final Object fieldValue) throws AssertException, DeserializingException {
        AssertHelpers.notNull(target, "target");
        AssertHelpers.notNull(fieldValue, "fieldValue");
        AssertHelpers.isTrue(fieldValue instanceof Collection, "Cannot use collection codec on non set field");
        try {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) getCollectionClass().newInstance();

            @SuppressWarnings("unchecked")
            Collection<Object> fieldSet = (Collection<Object>) fieldValue;

            for (Object value : fieldSet) {
                if (value != null) {
                    addObjectToEnumCollection(collection, value);
                } else {
                    collection.add(null);
                }
            }

            ReflectionHelpers.setFieldValue(target, reflectionPath, collection);
        } catch (ReflectiveOperationException e) {
            throw new DeserializingException(e);
        }
    }

    private void addObjectToEnumCollection(Collection<Object> collection, Object value) throws AssertException {
        AssertHelpers.isTrue(value instanceof String, "Trying to decode something other than a String into an Enum");
        collection.add(Enum.valueOf(modelClass, (String) value));
    }

    @Override
    public void initialize(Column annotation) throws AssertException {
    }

}
