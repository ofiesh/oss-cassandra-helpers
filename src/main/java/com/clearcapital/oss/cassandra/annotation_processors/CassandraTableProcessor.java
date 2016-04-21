package com.clearcapital.oss.cassandra.annotation_processors;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.util.Asserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.ColumnDefinition;
import com.clearcapital.oss.cassandra.JsonColumnDefinition;
import com.clearcapital.oss.cassandra.PlaceholderColumnDefinition;
import com.clearcapital.oss.cassandra.ReflectionColumnDefinition;
import com.clearcapital.oss.cassandra.RingClient;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.cassandra.configuration.AutoSchemaConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Support for processing {@link CassandraTable} annotated classes.
 * 
 * @author eehlinger
 */
public class CassandraTableProcessor {

    private static Logger log = LoggerFactory.getLogger(CassandraTableProcessor.class);

    static Map<CassandraTable, Map<String, ColumnDefinition>> columnDefinitionMaps = new HashMap<>();
    static Map<CassandraTable, Collection<ColumnDefinition>> columnDefinitionLists = new HashMap<>();

    /**
     * Given a {@code tableClass}, find and return the {@link CassandraTable} annotation.
     * 
     * @param tableClass
     * @return {@code @CassandraTable} annotation, if any; null otherwise.
     * @throws AssertException
     */
    public static CassandraTable getAnnotation(final Class<?> tableClass) throws AssertException {
        AssertHelpers.notNull(tableClass, "tableClass");

        CassandraTable annotation = tableClass.getAnnotation(CassandraTable.class);
        return annotation;
    }

    /**
     * Decode {@code annotation.columnDefinitions()} into a Collection<ColumnDefinition>
     * 
     * <p>
     * <b>Note</b> There is an optimization here to store a previously decoded map for each columnDefinitions table
     * class that we run across. The Collection<{@link ColumnDefinition}> objects will be reused repeatedly by decoding
     * methods.
     * </p>
     * 
     * @param annotation
     * @return a collection consisting of the ColumnDefinition objects provided by the enum constants in
     *         annotation.columnDefinitions()
     */
    public static Collection<ColumnDefinition> getColumnDefinitionList(final CassandraTable annotation) {
        if (!columnDefinitionLists.containsKey(annotation)) {
            cacheColumnDefinitions(annotation);
        }
        return columnDefinitionLists.get(annotation);
    }

    /**
     * Decode {@code annotation.columnDefinitions()} into a Map<String,ColumnDefinition>, where each entry's key is the
     * same as the value's {@code ColumnDefinition#getColumnName()}
     * 
     * <p>
     * <b>Note</b> There is an optimization here to store a previously decoded map for each columnDefinitions table
     * class that we run across. The Collection<{@link ColumnDefinition}> objects will be reused repeatedly by decoding
     * methods.
     * </p>
     * 
     * @param annotation
     * @return a collection consisting of the ColumnDefinition objects provided by the enum constants in
     *         annotation.columnDefinitions()
     */
    public static Map<String, ColumnDefinition> getColumnDefinitionMap(final CassandraTable annotation) {
        if (!columnDefinitionMaps.containsKey(annotation)) {
            cacheColumnDefinitions(annotation);
        }
        return columnDefinitionMaps.get(annotation);
    }

    /**
     * Create a TableBuilder
     */
    public static TableBuilder tableBuilder(CommandExecutor executor, MultiRingClientManager manager,
            final Class<?> tableClass) throws AssertException {
        return new TableBuilder(executor, manager, tableClass);
    }

    public static TableComparator tableComparator(CommandExecutor executor, MultiRingClientManager client,
            Class<?> tableClass) throws AssertException {
        return new TableComparator(executor, client, tableClass);
    }

    public static SchemaComparator schemaComparator(CommandExecutor executor, MultiRingClientManager client,
            AutoSchemaConfiguration autoSchemaConfiguration) {
        return new SchemaComparator(executor, client, autoSchemaConfiguration);
    }

    private static void cacheColumnDefinitions(final CassandraTable annotation) {
        Asserts.notNull(annotation, "Annotation");
        ImmutableMap.Builder<String, ColumnDefinition> mapBuilder = ImmutableMap.<String, ColumnDefinition> builder();
        ImmutableList.Builder<ColumnDefinition> listBuilder = ImmutableList.<ColumnDefinition> builder();
        for (Column column : annotation.columns()) {
            if (column.reflectionColumnInfo().isSelected()) {
                ReflectionColumnDefinition reflectionColumn = ReflectionColumnDefinition.builder()
                        .fromAnnotation(column).build();
                listBuilder.add(reflectionColumn);
                mapBuilder.put(reflectionColumn.getColumnName().toLowerCase(), reflectionColumn);
            } else if (column.jsonColumnInfo().isSelected()) {
                JsonColumnDefinition jsonColumn = JsonColumnDefinition.builder().fromAnnotation(column).build();
                listBuilder.add(jsonColumn);
                mapBuilder.put(jsonColumn.getColumnName().toLowerCase(), jsonColumn);
            } else if (column.createdElsewhere()) {
                PlaceholderColumnDefinition placeholderColumn = PlaceholderColumnDefinition.builder()
                        .fromAnnotation(column).build();
                listBuilder.add(placeholderColumn);
                mapBuilder.put(placeholderColumn.getColumnName().toLowerCase(), placeholderColumn);
            }
        }
        ImmutableList<ColumnDefinition> list = listBuilder.build();
        ImmutableMap<String, ColumnDefinition> map = mapBuilder.build();
        if (log.isDebugEnabled()) {
            log.debug("Caching columnDefinitionList:" + list);
            log.debug("Caching columnMap:" + map);
        }
        columnDefinitionLists.put(annotation, list);
        columnDefinitionMaps.put(annotation, map);
    }


    public static void dropTableIfExists(MultiRingClientManager clientManager, final Class<?> tableClass) throws AssertException, CassandraException {
    	CassandraTable annotation = getAnnotation(tableClass);
    	RingClient client = clientManager.getRingClientForGroup(annotation.multiRingGroup());
    	client.getPreferredKeyspace().dropTableIfExists(annotation.tableName());
    }
}
