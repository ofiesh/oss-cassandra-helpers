package com.clearcapital.oss.cassandra.iterate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;

import com.clearcapital.oss.cassandra.SessionHelper;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.SelectionOrAlias;

public class CassandraTableWalker<E> implements Iterable<E> {

    private final SessionHelper session;

    private PreparedStatement read;
    private final CassandraRowDeserializer<E> deserializer;
    private final String tableName;
    private final String[] keyColumnNames;
    private int fetchSize;
    private Long token;
    private Long startToken;
    private Long endToken;
    private String[] selectColumnNames;
    private ConsistencyLevel readConsistencyLevel;

    /**
     * TODO: Walker can be improved in following manner. Due to time constrain, not changing it as part of the current story.
     * <ul>
     * <li>No need to pass keyColumnNames. Now we have all Table Classes with annotations, it can easily be automatically
     * populated.</li> 
     * </ul>
     * 
     * @param session
     * @param keyspaceName
     * @param tableName
     * @param keyColumnNames
     * @param deserializer
     */
    public CassandraTableWalker(final SessionHelper session, final String tableName,
            final String[] keyColumnNames, final CassandraRowDeserializer<E> deserializer) {

        this.session = session;
        this.fetchSize = 100;
        this.tableName = tableName;
        this.keyColumnNames = keyColumnNames;
        this.deserializer = deserializer;
        this.read = null;
        this.startToken = Long.MIN_VALUE;
        this.endToken = Long.MAX_VALUE;
        this.readConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        this.selectColumnNames = null;
    }

    public CassandraTableWalker(final SessionHelper session, final String tableName,
            final List<?> keyColumnNames, final CassandraRowDeserializer<E> deserializer) {
        this.session = session;
        this.fetchSize = 100;
        this.tableName = tableName;
        this.keyColumnNames = keyColumnNames.toArray(new String[keyColumnNames.size()]);
        this.deserializer = deserializer;
        this.read = null;
        this.startToken = Long.MIN_VALUE;
        this.endToken = Long.MAX_VALUE;
        this.readConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        this.selectColumnNames = null;
    }

    public void buildStatements() {
        if (this.read != null) {
            return; // it's already built.
        }

        TableMetadata tableMetaData = session.getKeyspaceInfo().getTable(tableName);
        SelectionOrAlias readSelection = QueryBuilder.select().column(QueryBuilder.token(keyColumnNames));

        if (getSelectColumnNames() != null && getSelectColumnNames().length > 0) {
            for (String columnName : getSelectColumnNames()) {
                readSelection.column(columnName);
            }
        } else {
            for (ColumnMetadata column : tableMetaData.getColumns()) {
                if (column.getName() != null && !column.getName().isEmpty()) {
                    readSelection.column(column.getName());
                }
            }
        }

        RegularStatement read = readSelection.from(tableName)
                .where(QueryBuilder.gte(QueryBuilder.token(keyColumnNames), QueryBuilder.bindMarker()))
                .and(QueryBuilder.lte(QueryBuilder.token(keyColumnNames), QueryBuilder.bindMarker()));

        if (getReadConsistencyLevel() == null) {
            setReadConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        }
        read.setConsistencyLevel(getReadConsistencyLevel());
        read.setFetchSize(fetchSize);
        this.read = session.prepare(read);
    }

    public CassandraTableWalker<E> clearPreparedStatements() {
        this.read = null;
        return this;

    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public CassandraTableWalker<E> setFetchSize(final Integer value) {
        if (value != null) {
            fetchSize = value;
        } else {
            fetchSize = 100;
        }
        return clearPreparedStatements();
    }

    public CassandraTableWalker<E> setStartToken(final Long value) {
        if (value != null) {
            startToken = value;
        } else {
            startToken = Long.MIN_VALUE;
        }
        return clearPreparedStatements();
    }

    public CassandraTableWalker<E> setEndToken(final Long value) {
        if (value != null) {
            endToken = value;
        } else {
            endToken = Long.MAX_VALUE;
        }
        return clearPreparedStatements();
    }

    @Override
    public Iterator<E> iterator() {
        return new CassandraTableIterator<E>(this);
    }

    public SessionHelper getSession() {
        return session;
    }

    public PreparedStatement getReadStatement() {
        buildStatements();
        return read;
    }

    public CassandraRowDeserializer<E> getDeserializer() {
        return deserializer;
    }

    public Long getToken() {
        return token;
    }

    void setToken(final long value) {
        token = value;
    }

    public Long getStartToken() {
        return startToken;
    }

    public Long getEndToken() {
        return endToken;
    }

    public String[] getSelectColumnNames() {
        return selectColumnNames;
    }

    /**
     * Set SelectColumnNames for the select query. Note: It is advised to provide custom deserializer when you are only selecting
     * few columns from the table The default deserializer might throw an error.
     * 
     * @param selectColumnNames
     */
    public void setSelectColumnNames(final String[] selectColumnNames) {
        this.selectColumnNames = selectColumnNames;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    /**
     * For the table with the large sized record (e.g clearREResponse), QUORM consistency can cause performance problem. This is
     * intented to used with transformer/data processing commands only.
     * 
     * @param readConsistencyLevel
     */
    public void setReadConsistencyLevel(final ConsistencyLevel readConsistencyLevel) {
        this.readConsistencyLevel = readConsistencyLevel;
    }

    /**
     * Progress, as percentage of the specified token range.
     */
    public double getProgress() {
        double rangeSize = (double) getEndToken() - (double) getStartToken();
        double tokenOffset = (double) getToken() - (double) getStartToken();
        double progress = tokenOffset / rangeSize * 100.0;
        return progress;
    }

    /**
     * Estimated time of completion, given token range and time which has elapsed since {@code start}. 
     */
    public LocalDateTime getEta(final LocalDateTime start) {
    	Duration elapsed = Duration.between(start, LocalDateTime.now());
        long estTotal = (long) (elapsed.toMillis() / (getProgress() / 100.0));
        LocalDateTime eta = start.plus(estTotal,ChronoUnit.MILLIS);
        return eta;
    }

}
