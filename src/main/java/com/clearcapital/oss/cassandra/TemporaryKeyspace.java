package com.clearcapital.oss.cassandra;

import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.Session;

/**
 * <p>
 * Retain a {@link Session} object that is logged in to a keyspace. When
 * {@link #close()} is called, it will drop that keyspace.
 * </p>
 * 
 * <p>
 * There are admittedly very few use-cases for this:
 * </p>
 * 
 * <ul>
 * <li>Your application needs to create temporary tables, and you want to
 * guarantee that, even if the application crashes before
 * {@link TemporaryTable#close()} is called, the tables can be easily identified
 * and dropped. More easily and safely, for example, than by putting temporary
 * tables directly in one of the application's permanent keyspaces.</li>
 * 
 * <li>You are developing tests that need to be as independent from one another
 * as possible. Have each test create a unique keyspace, perhaps by using
 * {@link MultiRingClientManager#createTemporaryKeyspace(String, String)}, and
 * allow {@link TemporaryKeyspace} to clean up afterwards. This makes it easier
 * to write tests that can run in parallel.</li>
 * </ul>
 * 
 * @author eehlinger
 */
public class TemporaryKeyspace implements AutoCloseable {

	final SessionHelper session;

	public TemporaryKeyspace(SessionHelper session) throws AssertException {
		AssertHelpers.notNull(session, "keyspaceSession");
		AssertHelpers.notNull(session.getLoggedKeyspace(), "keyspaceSession.getLoggedKeyspace");
		this.session = session;
	}

	@Override
	public void close() throws Exception {
		session.dropKeyspace(session.getLoggedKeyspace());
	}

	public SessionHelper getSession() {
		return session;
	}

}
