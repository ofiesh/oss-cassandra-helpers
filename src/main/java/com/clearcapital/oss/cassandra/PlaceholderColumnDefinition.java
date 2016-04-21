package com.clearcapital.oss.cassandra;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.datastax.driver.core.DataType;
import com.google.common.base.MoreObjects;

/**
 * A definition for a column that is created outside of the annotation system,
 * e.g., by solr.
 */
public class PlaceholderColumnDefinition implements ColumnDefinition {

	private String columnName;
	private Column annotation;
	

	@Override
	public Column getAnnotation() {
		return annotation;
	}

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
				.add("isCreatedElsewhere", getIsCreatedElsewhere()).add("annotation", getAnnotation()).toString();
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
		
		public Builder setAnnotation(Column value) {
			result.annotation = value;
			return this;
		}

		public Builder fromAnnotation(Column column) {
			return setAnnotation(column).setColumnName(column.cassandraName());
		}
	}

	@Override
	public void encode(Map<String, Object> result, Object object) throws SerializingException {
		// NO-OP. This column will be populated (maybe?) by whatever force defined it. (e.g., DSE's solr integration?)
	}

}
