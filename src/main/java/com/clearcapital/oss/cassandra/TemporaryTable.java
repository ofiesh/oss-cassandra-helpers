package com.clearcapital.oss.cassandra;

public class TemporaryTable implements AutoCloseable {

    private SessionHelper session;
    private String tableName;

    public TemporaryTable(SessionHelper session, String tableName) {
        this.session = session;
        this.tableName = tableName;
    }

    public SessionHelper getSession() {
        return session;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public void close() throws Exception {
        session.dropTableIfExists(tableName);
    }

}
