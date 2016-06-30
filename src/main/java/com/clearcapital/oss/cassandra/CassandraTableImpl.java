package com.clearcapital.oss.cassandra;

import java.lang.reflect.ParameterizedType;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.util.Asserts;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;

import com.clearcapital.oss.cassandra.ColumnDefinition.ColumnOption;
import com.clearcapital.oss.cassandra.annotation_processors.CassandraTableProcessor;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.annotations.ReflectionColumnInfo;
import com.clearcapital.oss.cassandra.bundles.CassandraCommand;
import com.clearcapital.oss.cassandra.exceptions.CassandraDeserializationException;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.iterate.CassandraResultSetFilteredIterator;
import com.clearcapital.oss.cassandra.iterate.CassandraResultSetIterator;
import com.clearcapital.oss.cassandra.iterate.CassandraRowDeserializer;
import com.clearcapital.oss.cassandra.iterate.CassandraTableWalker;
import com.clearcapital.oss.cassandra.iterate.WalkerGenerator;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.DebuggableCommand;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.UncheckedAssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * A helper class for developing table classes
 *
 * @param <TableClass>
 *            - the derived, {@link CassandraTable}-annotated class
 * @param <ModelClass>
 *            - class to (de)code in Cassandra.
 */
public class CassandraTableImpl<TableClass, ModelClass>
        implements WalkerGenerator, CassandraRowDeserializer<ModelClass> {

    private final MultiRingClientManager multiRingClientManager;

    protected Map<String, ColumnDefinition> columnDefinitionMap;

    public CassandraTableImpl(MultiRingClientManager multiRingClientManager) {
        this.multiRingClientManager = multiRingClientManager;
    }

    public ModelClass deserializeRow(Row row) throws DeserializingException {
        if (row == null) {
            throw new DeserializingException("provided row is null.");
        }
        try {
            CassandraTable annotation = getAnnotation();
            Map<String, ColumnDefinition> columnDefinitionMap = CassandraTableProcessor
                    .getColumnDefinitionMap(annotation);

            ModelClass result = null;

            for (Definition column : row.getColumnDefinitions()) {
                try {
                    String columnName = column.getName();
                    ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName.toLowerCase());
                    if (columnDefinition != null) {

                        // This if-else-on-type cascade is a little bit of a
                        // smell; it would be better to have a single method in the
                        // ColumnDefinition interface, but the observable
                        // behavior intentionally differs depending on whether it's a
                        // Json column or not.

                        if (columnDefinition instanceof JsonColumnDefinition) {
                            ModelClass jsonResult = ((JsonColumnDefinition) columnDefinition).<ModelClass> decode(row,
                                    column);

                            return jsonResult;
                        } else {
                            if (result == null) {

                                @SuppressWarnings("unchecked")
                                final Class<ModelClass> targetClass = (Class<ModelClass>) annotation.modelClass();

                                result = targetClass.newInstance();
                            }
                            columnDefinition.decode(result, row, column);
                        } 
                    }

                } catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
                    throw new DeserializingException("Could not deserialize column:" + column.getName(), e);
                }
            }
            return result;
        } catch (AssertException | ReflectiveOperationException e) {
            throw new CassandraDeserializationException(e);
        }
    }

    public CassandraTable getAnnotation() throws AssertException {
        return CassandraTableProcessor.getAnnotation(getTableClass());
    }

    public Class<?> getModelClass() throws AssertException {
        CassandraTable annotation = CassandraTableProcessor.getAnnotation(getTableClass());
        return annotation.modelClass();
    }

    public MultiRingClientManager getMultiRingClientManager() {
        return multiRingClientManager;
    }

    public Class<TableClass> getTableClass() {
        ParameterizedType parameterizedType = ReflectionHelpers.getParameterizedType(getClass());
        UncheckedAssertHelpers.isTrue(parameterizedType.getActualTypeArguments()[0] instanceof Class<?>,
                "parameterizedType.getActualTypeArguments()[0] instanceof Class<?>");

        @SuppressWarnings("unchecked")
        Class<TableClass> result = (Class<TableClass>) parameterizedType.getActualTypeArguments()[0];
        return result;
    }

    public RingClient getRingClient() throws AssertException {
        String multiRingGroup = getAnnotation().multiRingGroup();
        RingClient ringClientForGroup = multiRingClientManager.getRingClientForGroup(multiRingGroup);
        return ringClientForGroup;
    }

    public SessionHelper getSession() throws AssertException {
        return getRingClient().getPreferredKeyspace();
    }

    public String getTableName() throws AssertException {
        return getAnnotation().tableName();
    }

    /**
     * Prepare the given {@code statement}
     */
    public PreparedStatement prepareStatement(RegularStatement statement) throws AssertException {
        return getSession().prepare(statement);
    }

    /**
     * Read the first record for {@code statement}, deserialized using {@code this}. If no records are returned from
     * Cassandra, return null.
     * 
     * @throws AssertException
     * @throws CassandraException
     */
    public ModelClass readFirst(Statement statement) throws CassandraException, AssertException {
        return readFirst(statement, this);
    }

    /**
     * Read the first record for {@code statement}, deserialized using {@code deserializer}. If no records are returned
     * from Cassandra, return null.
     * 
     * @throws AssertException
     * @throws CassandraException
     */
    public <E> E readFirst(Statement statement, CassandraRowDeserializer<E> deserializer)
            throws CassandraException, AssertException {
        for (E item : readIterable(statement, deserializer)) {
            return item;
        }
        return null;
    }

    /**
     * Read the results of {@code statement} into a collection (ImmutableList).
     * 
     * <p>
     * <strong>NOTE:</strong> if this throws a NPE inside the {@code ImmutableList#copyOf(Iterable)} method, there is a
     * good chance that your table class has a {@link ReflectionColumnInfo#javaPath()} to a field that doesn't exist in
     * the {@link CassandraTable#modelClass()}. Check the console log for warnings about failure to deserialize row.
     * </p>
     */
    public Collection<ModelClass> readCollection(Statement statement) throws CassandraException, AssertException {
        return ImmutableList.<ModelClass> copyOf(readIterable(statement, this));
    }

    /**
     * Read the results of {@code statement} into a collection (ImmutableList).
     * 
     * <p>
     * <strong>NOTE:</strong> if this throws a NPE inside the {@code ImmutableList#copyOf(Iterable)} method, there is a
     * good chance that your table class has a {@link ReflectionColumnInfo#javaPath()} to a field that doesn't exist in
     * the {@link CassandraTable#modelClass()}.
     * </p>
     */
    public <E> Collection<E> readCollection(Statement statement, CassandraRowDeserializer<E> deserializer)
            throws CassandraException, AssertException {
        return ImmutableList.<E> copyOf(readIterable(statement, deserializer));
    }

    /**
     * Read the results of {@code statement} into a collection (ImmutableList).
     * 
     * <p>
     * <strong>NOTE:</strong> if this throws a NPE inside the {@code ImmutableList#copyOf(Iterable)} method, there is a
     * good chance that your table class has a {@link ReflectionColumnInfo#javaPath()} to a field that doesn't exist in
     * the {@link CassandraTable#modelClass()}.
     * </p>
     */
    public Collection<ModelClass> readFilteredCollection(Statement statement, Predicate<Row> predicate)
            throws CassandraException, AssertException {
        return ImmutableList.<ModelClass> copyOf(readFilteredIterable(statement, predicate, this));
    }

    /**
     * Read the results of {@code statement} into a collection (ImmutableList), after filtering the rows using
     * {@code predicate}.
     * 
     * <p>
     * <strong>NOTE:</strong> if this throws a NPE inside the {@code ImmutableList#copyOf(Iterable)} method, there is a
     * good chance that your table class has a {@link ReflectionColumnInfo#javaPath()} to a field that doesn't exist in
     * the {@link CassandraTable#modelClass()}.
     * </p>
     */
    public <E> Collection<E> readFilteredCollection(Statement statement, Predicate<Row> predicate,
            CassandraRowDeserializer<E> deserializer) throws CassandraException, AssertException {
        return ImmutableList.<E> copyOf(readFilteredIterable(statement, predicate, deserializer));
    }

    /**
     * Read the results of a statement in an iterable form
     * 
     * @throws AssertException
     * @throws CassandraException
     */
    public Iterable<ModelClass> readIterable(Statement statement) throws CassandraException, AssertException {
        return readIterable(statement, this);
    }

    public <E> Iterable<E> readIterable(Statement statement, CassandraRowDeserializer<E> deserializer)
            throws CassandraException, AssertException {
        ResultSet resultSet = getRingClient().getPreferredKeyspace().execute(statement);

        return new CassandraResultSetIterator<E>(resultSet, deserializer);
    }

    /**
     * Read the results of a statement in an iterable form
     * 
     * @throws AssertException
     * @throws CassandraException
     */
    public Iterable<ModelClass> readFilteredIterable(Statement statement, Predicate<Row> predicate)
            throws CassandraException, AssertException {
        return readFilteredIterable(statement, predicate, this);
    }

    public <E> Iterable<E> readFilteredIterable(Statement statement, Predicate<Row> predicate,
            CassandraRowDeserializer<E> deserializer) throws CassandraException, AssertException {
        ResultSet resultSet = getRingClient().getPreferredKeyspace().execute(statement);

        return new CassandraResultSetFilteredIterator<E>(resultSet, predicate, deserializer);
    }

    public boolean isExhausted(Statement statement) throws CassandraException, AssertException {
        ResultSet resultSet = getRingClient().getPreferredKeyspace().execute(statement);
        return resultSet.isExhausted();
    }

    protected Map<String, Object> getFields(final ModelClass object) throws AssertException, SerializingException {
        AssertHelpers.notNull(getTableClass(), "tableClass");
        AssertHelpers.notNull(object, "object");
        CassandraTable annotation = getAnnotation();
        Map<String, Object> result = new TreeMap<String, Object>();

        Collection<ColumnDefinition> columnDefinitions = CassandraTableProcessor
                .getColumnDefinitionList(annotation);
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            columnDefinition.encode(result, object);
        }

        return result;
    }

    protected PreparedStatement prepareInsertStatement(final ConsistencyLevel consistencyLevel) throws AssertException {
        return prepareInsertStatement(consistencyLevel, null, null, false);
    }

    protected PreparedStatement prepareInsertStatement(final ConsistencyLevel consistencyLevel, final TimeUnit timeUnit,
            final Integer ttlDuration, boolean withDynamicTTL) throws AssertException {
        CassandraTable annotation = getAnnotation();
        AssertHelpers.notNull(annotation, "tableClass must have @CassandraTable annotation");
        Map<String, ColumnDefinition> columns = CassandraTableProcessor.getColumnDefinitionMap(annotation);
        // Map<String, CassandraColumnDefinition> additionalColumnsJson =
        // getAdditionalColumnsFromJson(annotation);

        TreeSet<String> columnNames = Sets.newTreeSet(// @formatter:off
            Iterables.transform(
                Iterables.filter(columns.entrySet(), new Predicate<Entry<String,ColumnDefinition>>(){
                    @Override
                    public boolean apply(Entry<String, ColumnDefinition> input) {
                        return input.getValue().getIsIncludedInInsertStatement();
                    }}), 
                new Function<Entry<String,ColumnDefinition>,String>() {
                    @Override
                    public String apply(Entry<String, ColumnDefinition> input) {
                        return input.getKey();
                    }}));// @formatter:on

        String[] columnNamesArray = columnNames.toArray(new String[0]);
        Object[] bindMarkers = new Object[columnNames.size()];
        Arrays.fill(bindMarkers, QueryBuilder.bindMarker());

        Insert insert = QueryBuilder.insertInto(annotation.tableName()).values(columnNamesArray, bindMarkers);
        if (withDynamicTTL) {
            insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
        } else if (TTLHelpers.isValidTTL(timeUnit, ttlDuration)) {
            insert.using(QueryBuilder.ttl(TTLHelpers.getTTL(timeUnit, ttlDuration)));
        }
        insert.setConsistencyLevel(consistencyLevel);
        return getRingClient().getPreferredKeyspace().prepare(insert);
    }

    /**
     * Prepare the given {@code statement} at the given {@code consistencyLevel}
     */
    protected PreparedStatement prepareStatement(RegularStatement statement, ConsistencyLevel consistencyLevel)
            throws AssertException {
        statement.setConsistencyLevel(consistencyLevel);
        return prepareStatement(statement);
    }

    protected PreparedStatement deleteStatement(ConsistencyLevel consistency) throws AssertException {
        CassandraTable annotation = getAnnotation();
        Asserts.notNull(annotation, "tableClass must have @CassandraTable annotation");
        Map<String, ColumnDefinition> columns = CassandraTableProcessor.getColumnDefinitionMap(annotation);
        Delete delete = QueryBuilder.delete().from(annotation.tableName());
        delete.setConsistencyLevel(consistency);
        for (Entry<String, ColumnDefinition> pk : columns.entrySet()) {
            // @formatter:off
            // add a where clause for every column that is a partition or clustering column
            if  (   pk.getValue() != null 
                &&  (   ColumnOption.PARTITION_KEY.equals(pk.getValue().getColumnOption())
                    ||  ColumnOption.CLUSTERING_KEY_ASC.equals(pk.getValue().getColumnOption())
                    ||  ColumnOption.CLUSTERING_KEY_DESC.equals(pk.getValue().getColumnOption())
                    )
                ) {
                // @formatter:on
                delete.where(QueryBuilder.eq(pk.getKey(), QueryBuilder.bindMarker()));
            }
        }
        return getRingClient().getPreferredKeyspace().prepare(delete);
    }

    protected PreparedStatement deleteAllStatement(ConsistencyLevel consistency) throws AssertException {
        CassandraTable annotation = getAnnotation();
        Asserts.notNull(annotation, "tableClass must have @CassandraTable annotation");
        Map<String, ColumnDefinition> columns = CassandraTableProcessor.getColumnDefinitionMap(annotation);
        Delete delete = QueryBuilder.delete().from(annotation.tableName());
        delete.setConsistencyLevel(consistency);
        for (Entry<String, ColumnDefinition> pk : columns.entrySet()) {
            // @formatter:off
            // add a where clause for every partition column
            if  (   pk.getValue() != null 
                &&  ColumnOption.PARTITION_KEY.equals(pk.getValue().getColumnOption())
                ) {
                // @formatter:on
                delete.where(QueryBuilder.eq(pk.getKey(), QueryBuilder.bindMarker()));
            }
        }
        return getRingClient().getPreferredKeyspace().prepare(delete);
    }

    protected Statement updateStatement(final Map<String, Object> fields, final List<String> forcedFields)
            throws AssertException {
        if (forcedFields != null) {
            // Remove all fields which are *not* in forcedFields and which *are* null.
            Set<Map.Entry<String, Object>> entries = fields.entrySet();
            Set<String> removeKeys = new HashSet<String>();
            for (Entry<String, Object> entry : entries) {
                if (forcedFields.contains(entry.getKey())) {
                    continue;
                }
                if (entry.getValue() == null) {
                    removeKeys.add(entry.getKey());
                }
            }
            fields.keySet().removeAll(removeKeys);
        } else {
            // remove all null fields.
            fields.values().removeAll(Collections.singleton(null));
        }

        TableMetadata table = getSession().getTableMetadata(getTableName());

        String[] fieldNames = new String[fields.keySet().size()];
        fields.keySet().toArray(fieldNames);
        Object[] values = fields.values().toArray();

        Insert insert = QueryBuilder.insertInto(table).values(fieldNames, values);

        return insert;
    }

    private Statement updateStatement(final ModelClass model, final List<String> forcedFields)
            throws ReflectionPathException, AssertException, SerializingException {
        Map<String, Object> fields = getFields(model);
        return updateStatement(fields, forcedFields);
    }

    public DebuggableCommand updateCommand(final ModelClass model, final List<String> forcedFields)
            throws ReflectionPathException, AssertException, SerializingException {
        return CassandraCommand.builder(getSession()).setStatement(updateStatement(model, forcedFields)).build();
    }

    public CassandraTableWalker.Builder<ModelClass> getWalker() throws AssertException {
        return getWalker(this);
    }

    public <E> CassandraTableWalker.Builder<E> getWalker(final CassandraRowDeserializer<E> customDeserializer)
            throws AssertException {
        Collection<ColumnDefinition> columnDefinitions = CassandraTableProcessor
                .getColumnDefinitionList(getAnnotation());

        Iterable<String> keyColumnNames = Iterables
                .transform(Iterables.filter(columnDefinitions, new Predicate<ColumnDefinition>() {

                    @Override
                    public boolean apply(ColumnDefinition object) {
                        return ColumnOption.PARTITION_KEY.equals(object.getColumnOption());
                    }
                }), new Function<ColumnDefinition, String>() {

                    @Override
                    public String apply(ColumnDefinition input) {
                        return input.getColumnName();
                    }
                });

        return CassandraTableWalker.<E> builder().setSession(getSession()).setTableName(getTableName())
                .setKeyColumnNames(keyColumnNames).setDeserializer(customDeserializer);
    }

    public URI getSolrQueryUri() throws AssertException {
        return getRingClient().getPreferredKeyspace().getSolrResourceUri(getTableName());
    }

    
    protected boolean existsAndIsNotNull(Row row, String fieldName) {
        return RowHelpers.existsAndIsNotNull(row,fieldName);
    }

    protected boolean existsAndIsNotNull(SolrDocument doc, String fieldName) {
        UncheckedAssertHelpers.notNull(doc, "doc may not be null");
        UncheckedAssertHelpers.isTrue(StringUtils.isNotBlank(fieldName), "field may not be null");
        return doc.containsKey(fieldName) && (doc.get(fieldName) != null);
    }

    public void addField(Map<String, Object> fields, final String columnName, final Boolean value) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();
        if (DataType.cboolean().equals(dataType)) {
            fields.put(columnName, value);
        } else {
            throw new IllegalArgumentException("Cannot assign Boolean value to non-Boolean Cassandra column type.");
        }
    }

    public void addField(Map<String, Object> fields, final String columnName, final Double value) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();
        if (DataType.cdouble().equals(dataType)) {
            fields.put(columnName, value);
        } else {
            throw new IllegalArgumentException("Cannot assign Double value to non-Double Cassandra column type.");
        }
    }

    public void addField(Map<String, Object> fields, final String columnName, int value) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();
        if (DataType.cint().equals(dataType)) {
            fields.put(columnName, value);
        } else {
            throw new IllegalArgumentException("Cannot assign int value to non-Int Cassandra column type.");
        }
    }

    public void addField(Map<String, Object> fields, final String columnName, final Integer value) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();
        if (DataType.cint().equals(dataType)) {
            fields.put(columnName, value);
        } else {
            throw new IllegalArgumentException("Cannot assign Integer value to non-Int Cassandra column type.");
        }
    }

    public void addField(Map<String, Object> fields, final String columnName, final Long value) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();
        if (DataType.bigint().equals(dataType) || DataType.cdouble().equals(dataType)) {
            fields.put(columnName, value);
        } else {
            throw new IllegalArgumentException("Cannot assign Long value to non-Bigint/Double Cassandra column type.");
        }
    }

    public void addField(Map<String, Object> fields, final String columnName, final String value) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();
        if (DataType.cdouble().equals(dataType)) {
            fields.put(columnDefinition.getColumnName(), NumberUtils.createDouble(value));
        } else if (DataType.cint().equals(dataType)) {
            fields.put(columnDefinition.getColumnName(), NumberUtils.createInteger(value));
        } else if (DataType.text().equals(dataType)) {
            fields.put(columnDefinition.getColumnName(), value);
        } else {
            throw new IllegalArgumentException(
                    "Cannot assign String value to non-Double/Int/Text Cassandra column type.");
        }
    }

    public <T> void addField(Map<String, Object> fields, final String columnName, final Map<String, T> value,
            final Class<T> mapType) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();

        if (mapType.equals(Boolean.class)) {
            if (DataType.map(DataType.text(), DataType.cboolean()).equals(dataType)) {
                fields.put(columnName, value);
            } else {
                throw new IllegalArgumentException(
                        "Field:" + columnName + " expected " + dataType + " got map<text,boolean>");
            }
        }

        if (mapType.equals(Double.class)) {
            if (DataType.map(DataType.text(), DataType.cdouble()).equals(dataType)) {
                fields.put(columnName, value);
            } else {
                throw new IllegalArgumentException(
                        "Field:" + columnName + " expected " + dataType + " got map<text,double>");
            }
        }

        if (mapType.equals(String.class)) {
            if (DataType.map(DataType.text(), DataType.text()).equals(dataType)) {
                fields.put(columnName, value);
            } else {
                throw new IllegalArgumentException(
                        "Field:" + columnName + " expected " + dataType + " got map<text,string>");
            }
        }
    }

    public <T> void addField(Map<String, Object> fields, final String columnName, final Set<T> value,
            final Class<T> mapType) {
        ColumnDefinition columnDefinition = columnDefinitionMap.get(columnName);
        DataType dataType = columnDefinition.getDataType();

        if (mapType.equals(Boolean.class)) {
            if (DataType.set(DataType.cboolean()).equals(dataType)) {
                fields.put(columnName, value);
            } else {
                throw new IllegalArgumentException(
                        "Column " + columnName + ": expected " + dataType + " got set<boolean>");
            }
        }

        if (mapType.equals(Double.class)) {
            if (DataType.set(DataType.cdouble()).equals(dataType)) {
                fields.put(columnName, value);
            } else {
                throw new IllegalArgumentException(
                        "Column " + columnName + ": expected " + dataType + " got set<double>");
            }
        }

        if (mapType.equals(String.class)) {
            if (DataType.set(DataType.text()).equals(dataType)) {
                fields.put(columnName, value);
            } else {
                throw new IllegalArgumentException(
                        "Column " + columnName + ": expected " + dataType + " got set<boolean>");
            }
        }
    }

    public static String getGeolocationString(final Double latitude, final Double longitude) {
        if (latitude == null || longitude == null) {
            return null;
        }
        return "" + latitude + "," + longitude;
    }

    public static void addRangeQueryFilter(final SolrQuery query, final String fieldName, final Object[] range) {
        if (range != null && (range[0] != null || range[1] != null)) {
            String lowerBound = range[0] != null ? range[0].toString() : "*";
            String upperBound = range[1] != null ? range[1].toString() : "*";
            query.addFilterQuery(fieldName + ":[" + lowerBound + " TO " + upperBound + "]");
        }
    }

    public static void addRangeQueryFilter(final SolrQuery query, final String fieldName, final Object[] range,
            final boolean includeNulls) {
        if (includeNulls) {
            if (range != null && (range[0] != null || range[1] != null)) {
                String lowerBound = range[0] != null ? range[0].toString() : "*";
                String upperBound = range[1] != null ? range[1].toString() : "*";
                query.addFilterQuery("(-" + fieldName + ":[* TO *] AND *:*) OR " + fieldName + ":[" + lowerBound
                        + " TO " + upperBound + "]");
                // query.addFilterQuery("(-" + fieldName + ":[* TO *]) OR " + fieldName + ":[" + lowerBound + " TO "
                // + upperBound + "]");
            }
        } else {
            addRangeQueryFilter(query, fieldName, range);
        }
    }

}
