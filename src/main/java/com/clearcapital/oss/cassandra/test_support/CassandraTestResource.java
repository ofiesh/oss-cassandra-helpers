package com.clearcapital.oss.cassandra.test_support;

import static org.junit.Assert.assertNotNull;

import java.util.Collection;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.configuration.MultiRingConfiguration;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.json.JsonSerializer;
import com.eaio.uuid.UUID;
import com.thenewentity.utils.dropwizard.MultipleConfigurationMerger;

/**
 * Provide a JUnit-compatible ExternalResource which understand {@link MultiRingClientManager} and supports using
 * Dropwizard-Multi-Config to specify the location of the various CassandraRings which a test may want access to.
 * 
 * @author eehlinger
 *
 */
public class CassandraTestResource extends ExternalResource {

	public static Logger log = LoggerFactory.getLogger(CassandraTestResource.class);

	public final MultiRingClientManager multiRingClientManager;

	public CassandraTestResource(Collection<String> configPaths) {
		multiRingClientManager = buildClient(configPaths);
	}

	private static MultiRingClientManager buildClient(Collection<String> configPaths) {
		try {
            // @formatter:off
            MultipleConfigurationMerger merger = MultipleConfigurationMerger.builder()
                    .setObjectMapper(JsonSerializer.getInstance().getObjectMapper())
                    .build();
            // @formatter:on

			MultiRingConfiguration config = merger.loadConfigs(configPaths,
					MultiRingConfiguration.class);
			return new MultiRingClientManager(config);
		} catch (AssertException e) {
			log.error("Failed to buildClient", e);
			return null;
		}

	}

	public MultiRingClientManager getClient() {
		return multiRingClientManager;
	}

	public String getUniqueName(String prefix) {
        String result = prefix + (new UUID()).toString().replace("-", "_");
        return result;
    }

	@Override
	protected void before() throws Throwable {
		assertNotNull(multiRingClientManager);
	}
}
