package com.clearcapital.oss.cassandra.codecs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.clearcapital.core.util.AssertUtils;
import com.clearcapital.core.util.ReflectionHelpers;
import com.clearcapital.core.util.exceptions.CoreException;
import com.clearcapital.core.util.exceptions.CoreExceptionFactory;
import com.clearcapital.core.util.json.JsonSerializer;
import com.clearcapital.oss.java.AssertHelpers;

/*
 * A codec that can serialize and deserialize between a cql type {@code set<text>} and a Java type
 * {@link Collection<?>}. The Java type is determined by the {@link CassandraColumnDefinition} {@code jsonType}.
 *
 * {@code CollectionCodec} should never used for {@link Collection<String>}.
 *
 * {@code CollectionCodec} is thread safe.
 */
public class CollectionCodec implements CassandraColumnCodec {

    private final Class<? extends Collection> collectionClass;

    public CollectionCodec() {
        this(ArrayList.class);
    }

    /*
     * @param collectionClass the class of the {@link Collection} the codec should instantiate. The class must have a
     * default constructor and may not be null.
     */
    public CollectionCodec(final Class<? extends Collection> collectionClass) {
        if(collectionClass == null) {

            //Can't use a checked exception here since class will be created in enum instantiation.
            //RuntimeException will be thrown on service start up and any tests  found Immediately
            throw new IllegalArgumentException("collectionClass in CollectionCodec cannot be null");
        }
        this.collectionClass = collectionClass;
    }

    @Override
    public void encodeColumn(final Map<String, Object> target, final Object sourceObject){
        AssertHelpers.notNull(target, "target");
        AssertHelpers.notNull(sourceObject, "sourceObject");

        Collection<Object> collection = null;

        try {
            Object fieldSet = ReflectionHelpers.getFieldValue(sourceObject, columnDefinition.getJavaPath());

            if (fieldSet != null) {
                collection = collectionClass.newInstance();
                AssertUtils.isTrue(fieldSet instanceof Collection, "Cannot use collection codec on non set field");
                if (fieldSet instanceof Collection) {
                    for (Object item : (Collection) fieldSet) {
                        if (item instanceof Enum) {
                            collection.add(item.toString());
                        } else {
                            collection.add(JsonSerializer.getInstance().getStringRepresentation(item));
                        }
                    }
                }
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw CoreExceptionFactory.createException("Unable to create new instance of " + collectionClass.getName(),
                    e);
        }

        target.put(columnDefinition.getCassandraName(), collection);
    }

    @Override
    public void decodeColumn(final Object target, final Object fieldValue) {
        AssertHelpers.notNull(target, "target");
        AssertHelpers.notNull(fieldValue, "fieldValue");

        try {
            Collection<Object> collection = collectionClass.newInstance();

            for (Object value : (Collection) fieldValue) {
                if (value != null) {
                    if (Enum.class.isAssignableFrom(columnDefinition.getJsonType())) {
                        collection.add(Enum.valueOf((Class) columnDefinition.getJsonType(), (String) value));
                    } else {
                        collection.add(JsonSerializer.getInstance().getObject((String) value, columnDefinition.getJsonType()));
                    }
                } else {
                    collection.add(null);
                }
            }

            ReflectionHelpers.setFieldValue(target, columnDefinition.getJavaPath(), collection);
        } catch (InstantiationException | IllegalAccessException e) {
            throw CoreExceptionFactory.createException("Unable to create new instance of " + collectionClass.getName(),
                    e);
        }
    }
}
