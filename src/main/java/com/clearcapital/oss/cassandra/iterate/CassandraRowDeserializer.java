package com.clearcapital.oss.cassandra.iterate;

import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.datastax.driver.core.Row;

public interface CassandraRowDeserializer<E> {

    E deserializeRow(Row row) throws DeserializingException;

}
