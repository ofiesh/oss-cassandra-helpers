package com.clearcapital.oss.cassandra;

import com.clearcapital.oss.cassandra.test_support.CassandraTestResource;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

/**
 * <p>
 * Retain a {@link Session} object that is logged in to a keyspace. When {@link #close()} is called, it will drop that
 * keyspace.
 * </p>
 * 
 * <p>
 * There are admittedly very few use-cases for this:
 * </p>
 * 
 * <li>Your application needs to create temporary tables, and you want to guarantee that, even if the application
 * crashes before {@link #close()} is called, the keyspace can be easily identified and dropped. More easily and safely,
 * for example, than by putting temporary tables directly in the primary keyspace.</li>
 * 
 * <li>You are developing tests that need to be as independent from one another as possible. Have each test create a
 * unique keyspace, perhaps by using {@link CassandraTestResource#getUniqueKeyspace}, and allow
 * {@link CassandraTemporaryKeyspace} to clean up afterwards. This makes it easier to write tests that can run in
 * parallel.</li>
 * 
 * @author eehlinger
 */
public class CassandraTemporaryKeyspace implements AutoCloseable {

    final SessionHelper session;
	
    public CassandraTemporaryKeyspace(SessionHelper session) throws AssertException {
		AssertHelpers.notNull(session, "keyspaceSession");
		AssertHelpers.notNull(session.getLoggedKeyspace(), "keyspaceSession.getLoggedKeyspace");
		this.session = session;
	}
	
	@Override
	public void close() throws Exception {
        session.execute(new SimpleStatement("DROP KEYSPACE " + session.getLoggedKeyspace()));
	}

    public SessionHelper getSession() {
		return session;
	}

}
