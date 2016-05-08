package com.clearcapital.oss.cassandra.iterate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;

import com.clearcapital.oss.cassandra.SessionHelper;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.SelectionOrAlias;
import com.google.common.collect.ImmutableList;

public class CassandraTableWalker<E> implements Iterable<E> {

    private SessionHelper session;

    private PreparedStatement readStatement;
    private CassandraRowDeserializer<E> deserializer;
    private String tableName;
    private String[] keyColumnNames;
    private int fetchSize;
    private Long token;
    private Long startToken;
    private Long endToken;
    private String[] selectColumnNames;
    private ConsistencyLevel readConsistencyLevel;

    static public <E> Builder<E> builder() throws AssertException {
        return new Builder<E>();
    }

    /**
     * Estimated time of completion, given token range and time which has elapsed since {@code start}.
     */
    public LocalDateTime getEta(final LocalDateTime start) {
        Duration elapsed = Duration.between(start, LocalDateTime.now());
        long estTotal = (long) (elapsed.toMillis() / (getProgress() / 100.0));
        LocalDateTime eta = start.plus(estTotal, ChronoUnit.MILLIS);
        return eta;
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

    @Override
    public Iterator<E> iterator() {
        return new CassandraTableIterator<E>(this);
    }

    static public class Builder<E> {

        CassandraTableWalker<E> result;

        Builder() throws AssertException {
            result = new CassandraTableWalker<E>();
            result.readStatement = null;
            setFetchSize(100);
            setStartToken(Long.MIN_VALUE);
            setEndToken(Long.MAX_VALUE);
            setReadConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            setSelectColumnNames((String[]) null);
        }

        public Builder<E> setDeserializer(CassandraRowDeserializer<E> value) {
            result.deserializer = value;
            return this;
        }

        public Builder<E> setEndToken(long value) {
            result.endToken = value;
            return this;
        }

        public Builder<E> setFetchSize(int value) {
            result.fetchSize = value;
            return this;
        }

        public Builder<E> setKeyColumnNames(Iterable<String> value) {
            List<String> asCollection = ImmutableList.<String> copyOf(value);
            return setKeyColumnNames(asCollection);
        }

        public Builder<E> setKeyColumnNames(List<String> value) {
            String[] asArray = value.toArray(new String[value.size()]);
            return setKeyColumnNames(asArray);
        }

        public Builder<E> setKeyColumnNames(String[] asArray) {
            result.keyColumnNames = asArray;
            return this;
        }

        /**
         * Tune the consistency of the reads.
         * @throws AssertException 
         */
        public Builder<E> setReadConsistencyLevel(ConsistencyLevel value) throws AssertException {
            AssertHelpers.notNull(value, "readConsistencyLevel");
            result.readConsistencyLevel = value;
            return this;
        }

        public Builder<E> setSelectColumnNames(Iterable<String> value) {
            List<String> asCollection = ImmutableList.<String> copyOf(value);
            return setSelectColumnNames(asCollection);
        }

        public Builder<E> setSelectColumnNames(List<String> value) {
            String[] asArray = value.toArray(new String[value.size()]);
            return setSelectColumnNames(asArray);
        }

        /**
         * Set SelectColumnNames for the select query. Note: It is advised to provide custom deserializer when you are
         * only selecting a few columns from the table.  The default deserializer might throw an error.
         */
        public Builder<E> setSelectColumnNames(String[] value) {
            result.selectColumnNames = value;
            return this;
        }

        public Builder<E> setSession(SessionHelper value) {
            result.session = value;
            return this;
        }

        public Builder<E> setStartToken(long value) {
            result.startToken = value;
            return this;
        }

        public Builder<E> setTableName(String value) {
            result.tableName = value;
            return this;
        }

        public CassandraTableWalker<E> build() {
            return result;
        }
    }

    CassandraRowDeserializer<E> getDeserializer() {
        return deserializer;
    }

    /**
     * NOTE: Used by CassandraTableIterator<E>
     */
    Long getEndToken() {
        return endToken;
    }

    /**
     * NOTE: Used by CassandraTableIterator<E>
     */
    PreparedStatement getReadStatement() {
        buildStatements();
        return readStatement;
    }

    /**
     * NOTE: Used by CassandraTableIterator<E>
     */
    SessionHelper getSession() {
        return session;
    }

    /**
     * NOTE: Used by CassandraTableIterator<E>
     */
    Long getStartToken() {
        return startToken;
    }

    /**
     * NOTE: Used by CassandraTableIterator<E>
     */
    int getFetchSize() {
        return fetchSize;
    }

    private CassandraTableWalker() {
    
    }

    private void buildStatements() {
        if (this.readStatement != null) {
            return; // it's already built.
        }
    
        TableMetadata tableMetaData = session.getKeyspaceInfo().getTable(tableName);
        SelectionOrAlias readSelection = QueryBuilder.select().column(QueryBuilder.token(keyColumnNames));
    
        if (selectColumnNames != null && selectColumnNames.length > 0) {
            for (String columnName : selectColumnNames) {
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
    
        read.setConsistencyLevel(readConsistencyLevel);
        read.setFetchSize(fetchSize);
        this.readStatement = session.prepare(read);
    }

    private Long getToken() {
        return token;
    }

    void setToken(long value) {
        token = value;
    }


}
