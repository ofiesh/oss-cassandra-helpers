package com.clearcapital.oss.cassandra.exceptions;

import com.clearcapital.oss.java.exceptions.DeserializingException;

public class CassandraDeserializationException extends DeserializingException {

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
