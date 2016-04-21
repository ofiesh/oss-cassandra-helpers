package com.clearcapital.oss.cassandra;

import java.util.Map;

import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.UncheckedAssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

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

	public void decode(Object target, Row row, Definition column) throws AssertException, ReflectiveOperationException{
		Object value = CQLHelpers.getColumn(row, column);
		ReflectionHelpers.setFieldValue(target, getReflectionPath(), value);
	}

	@Override
	public void encode(Map<String, Object> result, Object object) throws SerializingException, ReflectionPathException {
		Object value = ReflectionHelpers.getFieldValue(object, getReflectionPath());
		result.put(getColumnName(), value);
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
					.setReflectionPath(ImmutableList.<String> copyOf(columnAnnotation.reflectionColumnInfo().javaPath()))
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



}
