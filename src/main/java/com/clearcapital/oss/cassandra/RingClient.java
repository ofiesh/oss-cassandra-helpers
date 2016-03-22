package com.clearcapital.oss.cassandra;

import com.clearcapital.oss.cassandra.configuration.RingConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.test_support.CassandraTestResource;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;

public class RingClient {

    private final RingConfiguration configuration;
	private final Cluster cluster;

    public RingClient(RingConfiguration configuration) throws AssertException {
        AssertHelpers.notNull(configuration, "configuration");

        Cluster.Builder builder = new Cluster.Builder();
        for (String host : configuration.getHosts()) {
			builder.addContactPoint(host);
		}
        if (configuration.getPort() != null) {
            builder.withPort(configuration.getPort());
		}

        this.configuration = configuration;
        this.cluster = builder.build();
	}

    public SessionHelper getSession() {
        return new SessionHelper(cluster.connect(), configuration);
	}

    public SessionHelper getPreferredKeyspace() throws AssertException {
        return getKeyspace(getPreferredKeyspaceName());
    }

    public SessionHelper createPreferredKeyspace() throws AssertException, CassandraException {
        return createKeyspace(getPreferredKeyspaceName());
    }

    public SessionHelper createKeyspace(String keyspaceName) throws CassandraException {
        getSession().createKeyspace(keyspaceName);
        return getKeyspace(keyspaceName);
    }

    public String getPreferredKeyspaceName() throws AssertException {
        AssertHelpers.notNull(configuration.getPreferredKeyspace(), "configuration.preferredKeyspace");

        return configuration.getPreferredKeyspace();
    }

    public SessionHelper getKeyspace(String keyspaceName) {
        return new SessionHelper(cluster.connect(keyspaceName), configuration);
	}

	public KeyspaceMetadata getKeyspaceInfo(String keyspaceName) {
		return cluster.getMetadata().getKeyspace(keyspaceName);
	}

	public boolean keyspaceExists(String keyspaceName) {
		return null != getKeyspaceInfo(keyspaceName);
	}

	/**
	 * 
	 */
    public TemporaryKeyspace createTemporaryKeyspace(String keyspacePrefix)
            throws AssertException, CassandraException, InterruptedException {
        while (true) {
            synchronized (CassandraTestResource.class) {
                String keyspaceName = CQLHelpers.getUniqueName("tmp_");
                if (!keyspaceExists(keyspaceName)) {
                    CassandraTestResource.log.debug("Creating unique keyspace session:" + keyspaceName);
                    getSession().createKeyspace(keyspaceName);
                    return new TemporaryKeyspace(getKeyspace(keyspaceName));
                }
            }
            Thread.sleep(1);
        }
    }

	public void dropKeyspace() {
		// TODO Auto-generated method stub
		
	}

}
