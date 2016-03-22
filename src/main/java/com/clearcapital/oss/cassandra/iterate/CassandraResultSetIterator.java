package com.clearcapital.oss.cassandra.iterate;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CassandraResultSetIterator<E> implements Iterator<E> {

    private static Logger log = LoggerFactory.getLogger(CassandraResultSetIterator.class);
    private final Iterator<Row> iterator;
    private final CassandraRowDeserializer<E> deserializer;
    private Row row;

    CassandraResultSetIterator(ResultSet resultSet, CassandraRowDeserializer<E> deserializer) {
        this.iterator = resultSet.iterator();
        this.deserializer = deserializer;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public E next() {
        row = iterator.next();
        try {
            return deserializer.deserializeRow(row);
        } catch (CassandraException e) {
            log.warn("Could not deserializeRow",e);
            return null;
        }
    }

    public Row getRow() {
        return row;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
