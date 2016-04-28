package com.clearcapital.oss.cassandra.iterate;

import com.clearcapital.oss.cassandra.exceptions.CassandraDeserializationException;
import com.datastax.driver.core.Row;

public class CassandraRowDeserializeToRow implements CassandraRowDeserializer<Row> {
    
    private static CassandraRowDeserializeToRow instance = new CassandraRowDeserializeToRow();
    
    public static CassandraRowDeserializeToRow getInstance() {
        return instance;
    }

    @Override
    public Row deserializeRow(Row row) throws CassandraDeserializationException {
        return row;
    }

}
