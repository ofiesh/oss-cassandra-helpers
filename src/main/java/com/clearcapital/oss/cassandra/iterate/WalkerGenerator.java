package com.clearcapital.oss.cassandra.iterate;

import com.clearcapital.oss.java.exceptions.AssertException;

public interface WalkerGenerator {

    public <E> CassandraTableWalker.Builder<E> getWalker(final CassandraRowDeserializer<E> customDeserializer) throws AssertException;

}
