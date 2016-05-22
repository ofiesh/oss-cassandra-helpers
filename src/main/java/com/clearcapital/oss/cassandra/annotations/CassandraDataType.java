package com.clearcapital.oss.cassandra.annotations;

import com.datastax.driver.core.DataType;

public enum CassandraDataType {
    // simple types:
    BIGINT(DataType.bigint()),
    BLOB(DataType.blob()),
    BOOLEAN(DataType.cboolean()),
    COUNTER(DataType.counter()),
    DOUBLE(DataType.cdouble()),
    INT(DataType.cint()),
    TEXT(DataType.text()),
    TIMESTAMP(DataType.timestamp()),
    SET_TEXT(DataType.set(DataType.text())),
    LIST_TEXT(DataType.list(DataType.text())),
    MAP_TEXTTEXT(DataType.map(DataType.text(), DataType.text())),
    MAP_TEXTBIGINT(DataType.map(DataType.text(), DataType.bigint())),
    MAP_TEXTBOOLEAN(DataType.map(DataType.text(), DataType.cboolean())),
    MAP_TEXTDOUBLE(DataType.map(DataType.text(), DataType.cdouble())),
    SET_BOOLEAN(DataType.set(DataType.cboolean())),
    SET_DOUBLE(DataType.set(DataType.cdouble()));

    private final DataType dataType;

    CassandraDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public DataType getDataType() {
        return dataType;
    }
}
