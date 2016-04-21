package com.clearcapital.oss.cassandra;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.annotation_processors.CassandraTableProcessor;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.exceptions.CassandraDeserializationException;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.iterate.CassandraResultSetIterator;
import com.clearcapital.oss.cassandra.iterate.CassandraRowDeserializer;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.UncheckedAssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * A helper class for developing table classes
 *
 * @param <TableClass> - the derived, {@link CassandraTable}-annotated class
 * @param <ModelClass> - class to (de)code in Cassandra.
 */
public class CassandraTableImpl<TableClass, ModelClass> implements CassandraRowDeserializer<ModelClass> {

	private static Logger log = LoggerFactory.getLogger(CassandraTableImpl.class);

	private final MultiRingClientManager multiRingClientManager;

	public CassandraTableImpl(MultiRingClientManager multiRingClientManager) {
		this.multiRingClientManager = multiRingClientManager;
	}

	public MultiRingClientManager getMultiRingClientManager() {
		return multiRingClientManager;
	}

	public ParameterizedType getParameterizedType() {
		UncheckedAssertHelpers.isTrue(getClass().getGenericSuperclass() instanceof ParameterizedType,
				"getClass().getGenericSuperclass() instanceof ParameterizedType");
		ParameterizedType result = (ParameterizedType) getClass().getGenericSuperclass();
		return result;
	}

	public Class<?> getModelClass() throws AssertException {
		CassandraTable annotation = CassandraTableProcessor.getAnnotation(getTableClass());
		return annotation.modelClass();
	}

	public Class<TableClass> getTableClass() {
		ParameterizedType parameterizedType = getParameterizedType();
		UncheckedAssertHelpers.isTrue(parameterizedType.getActualTypeArguments()[0] instanceof Class<?>,
				"parameterizedType.getActualTypeArguments()[0] instanceof Class<?>");

		@SuppressWarnings("unchecked")
		Class<TableClass> result = (Class<TableClass>) parameterizedType.getActualTypeArguments()[0];
		return result;
	}

	public CassandraTable getAnnotation() throws AssertException {
		return CassandraTableProcessor.getAnnotation(getTableClass());
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

	public PreparedStatement prepareStatement(RegularStatement statement) throws AssertException {
		return getSession().prepare(statement);
	}

	public ModelClass deserializeRow(Row row) throws CassandraDeserializationException {
		if (row == null) {
			return null;
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
						// smell;
						// it would be better to have a single method in the
						// ColumnDefinition interface, but the observable
						// behavior
						// intentionally differs depending on whether it's a
						// Json
						// column or not.

						if (columnDefinition instanceof JsonColumnDefinition) {
							ModelClass jsonResult = ((JsonColumnDefinition) columnDefinition).<ModelClass> decode(row,
									column);

							return jsonResult;
						} else if (columnDefinition instanceof ReflectionColumnDefinition) {
							if (result == null) {

								@SuppressWarnings("unchecked")
								final Class<ModelClass> targetClass = (Class<ModelClass>) annotation.modelClass();

								result = targetClass.newInstance();
							}
							((ReflectionColumnDefinition) columnDefinition).decode(result, row, column);
						} else if (columnDefinition instanceof PlaceholderColumnDefinition) {
							// ignore
						} else {
							AssertHelpers.isTrue(false,
									"Unexpected ColumnDefinition subclass:" + ColumnDefinition.class.getName());
						}
					}

				} catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
					log.trace("Could not deserialize column:" + column.getName(), e);
					// Useful for debugging... throw new RuntimeException(e);
				}
			}
			return result;
		} catch (AssertException | ReflectiveOperationException | DeserializingException e) {
			throw new CassandraDeserializationException(e);
		}
	}

	protected PreparedStatement prepareInsertStatement(final ConsistencyLevel consistencyLevel) throws AssertException {
		return prepareInsertStatement(consistencyLevel, null, null);
	}

	protected PreparedStatement prepareInsertStatement(final ConsistencyLevel consistencyLevel, final TimeUnit timeUnit,
			final Integer ttlDuration) throws AssertException {
		CassandraTable annotation = getAnnotation();
		AssertHelpers.notNull(annotation, "tableClass must have @CassandraTable annotation");
		Map<String, ColumnDefinition> columns = CassandraTableProcessor.getColumnDefinitionMap(annotation);
		// Map<String, CassandraColumnDefinition> additionalColumnsJson =
		// getAdditionalColumnsFromJson(annotation);

		TreeSet<String> columnNames = new TreeSet<>();
		columnNames.addAll(columns.keySet());

		String[] columnNamesArray = columnNames.toArray(new String[0]);
		Object[] bindMarkers = new Object[columnNames.size()];
		Arrays.fill(bindMarkers, QueryBuilder.bindMarker());

		Insert insert = QueryBuilder.insertInto(annotation.tableName()).values(columnNamesArray, bindMarkers);
		if (TTLHelpers.isValidTTL(timeUnit, ttlDuration)) {
			insert.using(QueryBuilder.ttl(TTLHelpers.getTTL(timeUnit, ttlDuration)));
		}
		insert.setConsistencyLevel(consistencyLevel);
		return getRingClient().getPreferredKeyspace().prepare(insert);
	}

	/**
	 * Read the results of a statement into an immutable List
	 * 
	 * @throws AssertException
	 * @throws CassandraException
	 */
	public CassandraResultSetIterator<ModelClass> readList(Statement statement)
			throws CassandraException, AssertException {
		ResultSet resultSet = getRingClient().getPreferredKeyspace().execute(statement);

		return new CassandraResultSetIterator<ModelClass>(resultSet, this);
	}

	protected Map<String, Object> getFields(final Object object)
			throws AssertException, ReflectionPathException, SerializingException {
		AssertHelpers.notNull(getTableClass(), "tableClass");
		AssertHelpers.notNull(object, "object");

		CassandraTable annotation = getAnnotation();
		Map<String, Object> result = new TreeMap<String, Object>();

		Collection<ColumnDefinition> columnDefinitions = CassandraTableProcessor.getColumnDefinitionList(annotation);
		for (ColumnDefinition columnDefinition : columnDefinitions) {
			columnDefinition.encode(result, object);
		}

		return result;
	}

	protected PreparedStatement prepareStatement(RegularStatement statement, ConsistencyLevel consistencyLevel) throws AssertException {
		statement.setConsistencyLevel(consistencyLevel);
		return prepareStatement(statement);
	}

}
