package com.clearcapital.oss.cassandra;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.clearcapital.oss.json.JsonSerializer;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.base.MoreObjects;

/**
 * It turns out that it can be quite useful to have Cassandra tables of the
 * form: {id, json}.
 * 
 * In those cases, feel free to add a JsonColumnDefinition for the json column.
 */
public class JsonColumnDefinition implements ColumnDefinition {

	private String columnName;
	private Class<?> model;
	private Column annotation;

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
		return DataType.text();
	}

	@Override
	public ColumnOption getColumnOption() {
		return ColumnOption.NULL;
	}

	public Class<?> getModel() {
		return model;
	}

	public <T> T decode(Row row, Definition column) throws AssertException, DeserializingException {
		Object value = CQLHelpers.getColumn(row, column);
		AssertHelpers.isTrue(value instanceof String, "value instanceof String");

		@SuppressWarnings("unchecked")
		Class<T> targetClass = (Class<T>) getModel();

		return JsonSerializer.getInstance().<T> getObject((String) value, targetClass);
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

		public Builder setAnnotation(Column value) {
			result.annotation = value;
			return this;
		}

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
			return setAnnotation(column).setColumnName(column.cassandraName())
					.setModel(column.jsonColumnInfo().model());
		}
	}

	@Override
	public void encode(Map<String, Object> result, Object object) throws SerializingException {
		String asJson = JsonSerializer.getInstance().getStringRepresentation(object);
		result.put(getColumnName(), asJson);
	}

}
