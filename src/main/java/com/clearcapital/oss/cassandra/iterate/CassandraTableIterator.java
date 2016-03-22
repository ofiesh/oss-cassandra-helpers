package com.clearcapital.oss.cassandra.iterate;

import java.util.Iterator;

import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.datastax.driver.core.ResultSet;

public class CassandraTableIterator<E> implements Iterator<E> {

	private final CassandraTableWalker<E> walker;
	private CassandraResultSetIterator<E> iterator;

	public CassandraTableIterator(final CassandraTableWalker<E> cassandraTableWalker) {
		this.walker = cassandraTableWalker;
		this.iterator = null;
	}

	private CassandraResultSetIterator<E> needCurrentIterator() throws CassandraException {
		ResultSet resultSet = null;
		if (this.iterator == null) {
			/**
			 * Lesson learned. PreparedStatement interface doesn't have
			 * getFetchSize(). So when a prepared statement is generated of the
			 * regular statement, the fetchSize information is lost. So at the
			 * time of creation of boundStement it is set back to 0, big problem
			 * for query. So when we bind prepared statement, the fetchSize also
			 * needs to be set again.
			 */
			resultSet = walker.getSession().execute(walker.getReadStatement()
					.bind(walker.getStartToken(), walker.getEndToken()).setFetchSize(walker.getFetchSize()));
		}

		if (resultSet != null) {
			this.iterator = new CassandraResultSetIterator<E>(resultSet, walker.getDeserializer());
		}

		return this.iterator;
	}

	@Override
	public boolean hasNext() {
		try {
			CassandraResultSetIterator<E> iterator = needCurrentIterator();
			if (iterator != null) {
				return iterator.hasNext();
			} else {
				return false;
			}
		} catch (CassandraException e) {
			return false;
		}
	}

	@Override
	public E next() {
		try {
			CassandraResultSetIterator<E> iterator = needCurrentIterator();
			if (iterator != null) {
				E result = iterator.next(); // side effect: iterator remembers
											// next row as row()
				walker.setToken(iterator.getRow().getLong(0));
				return result;
			} else {
				return null;
			}
		} catch (CassandraException e) {
			return null;
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
