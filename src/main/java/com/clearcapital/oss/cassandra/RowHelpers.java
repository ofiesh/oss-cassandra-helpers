package com.clearcapital.oss.cassandra;

import org.apache.commons.lang3.StringUtils;

import com.clearcapital.oss.java.UncheckedAssertHelpers;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;

public class RowHelpers {

    static public boolean existsAndIsNotNull(Row row, String fieldName) {
        UncheckedAssertHelpers.notNull(row, "row may not be null");
        UncheckedAssertHelpers.isTrue(StringUtils.isNotBlank(fieldName), "fieldName may not be null");
        UncheckedAssertHelpers.notNull(row.getColumnDefinitions(), "row must have column definitions");
        return row.getColumnDefinitions().contains(fieldName) && !row.isNull(fieldName);
    }

    public static Object getColumn(final Row row, final Definition column) {
        UncheckedAssertHelpers.notNull(row, "row may not be null");
        UncheckedAssertHelpers.isTrue(StringUtils.isNotBlank(column.getName()), "fieldName may not be null");
        Object result = row.getObject(column.getName());
        return result;
    }

}
