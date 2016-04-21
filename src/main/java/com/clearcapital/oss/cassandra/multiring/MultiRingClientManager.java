package com.clearcapital.oss.cassandra.multiring;

import java.util.Map.Entry;

import com.clearcapital.oss.cassandra.TemporaryKeyspace;
import com.clearcapital.oss.cassandra.RingClient;
import com.clearcapital.oss.cassandra.configuration.MultiRingConfiguration;
import com.clearcapital.oss.cassandra.configuration.RingConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.google.common.collect.ImmutableMap;

/**
 * Maintain connections to the rings specified by {@link MultiRingConfiguration}, and provide access according to the
 * entries in {@link MultiRingConfiguration#getGroups()}.
 * 
 * @author eehlinger
 *
 */
public class MultiRingClientManager {

    private MultiRingClientMapper mapper;
	private ImmutableMap<String, RingClient> connections;

    public MultiRingClientManager(MultiRingConfiguration configuration) throws AssertException {
        mapper = new MultiRingClientMapper(configuration);
        connections = buildConnections(mapper);
	}

    public MultiRingClientMapper getMapper() {
        return mapper;
	}

	public ImmutableMap<String, RingClient> getRingClients() {
		return connections;
	}

	public RingClient getRingClientForRing(String key) throws AssertException {
		AssertHelpers.notNull(key, "key");
		AssertHelpers.isTrue(connections.containsKey(key), "connections.containsKey(\"" + key + "\"");
		return connections.get(key);
	}

	/**
	 * Find the ring for the specified groupKey. Note that if the specified groupKey is not configured,
	 * this will grab the configured default ring.
	 */
	public RingClient getRingClientForGroup(String groupKey) throws AssertException {
		String ringKey = mapper.getConnectionKeyForGroup(groupKey);
		return getRingClientForRing(ringKey);
	}

	public RingClient getDefaultRingClient() throws AssertException {
		return getRingClientForRing(mapper.getConfiguration().getDefaultRing());
	}
	
	
	/**
	 * Create a Temporary Keyspace in the given group, with the given prefix 
	 */
	public TemporaryKeyspace createTemporaryKeyspace(String group, String keyspacePrefix)
    		throws CassandraException, InterruptedException, AssertException {
        RingClient client = getRingClientForGroup(group);
        return client.createTemporaryKeyspace(keyspacePrefix);
    }
	
	public void disconnectAll() {
		for( RingClient ringClient : getRingClients().values()) {
			ringClient.disconnect();
		}
	}

    private static ImmutableMap<String, RingClient> buildConnections(
            MultiRingClientMapper mapper) throws AssertException {
		ImmutableMap.Builder<String, RingClient> connectionsBuilder = ImmutableMap
				.<String, RingClient> builder();

        if (null != mapper.getConfiguration().getRings()) {
            for (Entry<String, RingConfiguration> entry : mapper.getConfiguration().getRings().entrySet()) {
				RingClient client = new RingClient(entry.getValue());
				connectionsBuilder.put(entry.getKey(), client);
			}
		}
		ImmutableMap<String, RingClient> result = connectionsBuilder.build();
		return result;
	}
    

}
