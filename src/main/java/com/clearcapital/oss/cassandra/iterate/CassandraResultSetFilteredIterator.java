package com.clearcapital.oss.cassandra.iterate;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.java.exceptions.DeserializingException;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class CassandraResultSetFilteredIterator<E> implements Iterator<E>, Iterable<E> {

    private static Logger log = LoggerFactory.getLogger(CassandraResultSetFilteredIterator.class);
    private final Iterator<Row> iterator;
    private final CassandraRowDeserializer<E> deserializer;
    private Row row;

    public CassandraResultSetFilteredIterator(ResultSet resultSet, Predicate<Row> predicate,
            CassandraRowDeserializer<E> deserializer) {
        this.iterator = Iterators.filter(resultSet.iterator(), predicate);
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
        } catch (DeserializingException e) {
            log.warn("Could not deserializeRow", e);
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

    @Override
    public Iterator<E> iterator() {
        return this;
    }

}
