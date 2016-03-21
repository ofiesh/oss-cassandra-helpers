package com.clearcapital.oss.cassandra.exceptions;

public class CassandraException extends Exception {

    private static final long serialVersionUID = -5778624291709793532L;

    public CassandraException(Throwable cause) {
        super(cause);
    }

    public CassandraException(String message, Throwable cause) {
        super(message, cause);
    }
}
