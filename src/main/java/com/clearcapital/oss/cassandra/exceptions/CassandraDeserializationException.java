package com.clearcapital.oss.cassandra.exceptions;

public class CassandraDeserializationException extends CassandraException {

	private static final long serialVersionUID = 9098851226634344144L;

    public CassandraDeserializationException(Throwable cause) {
        super(cause);
    }

    public CassandraDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

	public CassandraDeserializationException(String message) {
		super(message);
	}
}
