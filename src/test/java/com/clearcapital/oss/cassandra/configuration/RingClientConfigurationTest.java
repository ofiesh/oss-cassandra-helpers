package com.clearcapital.oss.cassandra.configuration;

import static org.junit.Assert.*;

import org.junit.Test;

import com.clearcapital.oss.cassandra.configuration.RingConfiguration;
import com.clearcapital.oss.json.JsonSerializer;
import com.google.common.collect.ImmutableList;

public class RingClientConfigurationTest {
	
	@Test
	public void testEquals() {
		RingConfiguration configA = RingConfiguration.builder().build();
		RingConfiguration configB = RingConfiguration.builder().build();
		
		assertEquals(configA,configB);		

		configA = RingConfiguration.builder().setHosts(ImmutableList.<String> of("a","b")).build();
		configB = RingConfiguration.builder().build();

		assertNotEquals(configA,configB);

		configB = RingConfiguration.builder().setHosts(ImmutableList.<String> of("a","b")).build();
		
		assertEquals(configA,configB);		
	}

	@Test
	public void testJsonRoundTrip() throws Exception {
		RingConfiguration config = RingConfiguration.builder().setHosts(ImmutableList.<String> of("a","b")).build();
		
		String json = JsonSerializer.getInstance().getStringRepresentation(config);
		
		RingConfiguration readConfig = JsonSerializer.getInstance().getObject(json, RingConfiguration.class);
		
		assertEquals(config,readConfig);		
	}
	
}
