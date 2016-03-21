package com.clearcapital.oss.cassandra.annotation_processors;

import com.clearcapital.oss.cassandra.RingClient;
import com.clearcapital.oss.cassandra.SessionHelper;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;

import jersey.repackaged.com.google.common.base.Objects;

public class TableProcessor<T> {

    protected CommandExecutor executor;
    protected MultiRingClientManager manager;
    protected Class<?> tableClass;
    protected CassandraTable annotation;
    protected String tableName;
    protected String multiRingGroup;
    private RingClient ringClient;
    private SessionHelper session;
    private T self;

    TableProcessor(CommandExecutor executor, MultiRingClientManager manager, final Class<?> tableClass)
            throws AssertException {
        this.executor = executor;
        this.manager = manager;
        this.tableClass = tableClass;
        this.annotation = CassandraTableProcessor.getAnnotation(tableClass);
        AssertHelpers.notNull(annotation, "tableClass must have @CassandraTable annotation");
        this.tableName = annotation.tableName();
        this.multiRingGroup = annotation.multiRingGroup();
    }

    protected void setSelf(T value) {
        this.self = value;
    }

    public T setTableName(String value) {
        tableName = value;
        return self;
    }

    public T setMultiRingGroup(String value) {
        if (!Objects.equal(multiRingGroup, value)) {
            session = null;
            ringClient = null;
        }
        multiRingGroup = value;
        return self;
    }

    public T setSession(SessionHelper value) {
        session = value;
        return self;
    }

    protected RingClient getRingClient() throws AssertException {
        if (ringClient == null) {
            ringClient = manager.getRingClientForGroup(multiRingGroup);
        }
        return ringClient;
    }

    protected SessionHelper getSession() throws AssertException, CassandraException {
        if (session == null) {
            setSession(getRingClient().createPreferredKeyspace());
        }
        return session;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullTableName() throws AssertException, CassandraException {
        return getSession().getLoggedKeyspace() + "." + tableName;
    }

}