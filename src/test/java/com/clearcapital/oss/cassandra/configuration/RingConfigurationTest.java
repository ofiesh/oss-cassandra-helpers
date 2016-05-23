package com.clearcapital.oss.cassandra.configuration;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.clearcapital.oss.cassandra.policies.LoadBalancingPolicyEnum;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.io.Resources;

public class RingConfigurationTest {

    private static RingConfiguration ringConfiguration;

    @BeforeClass
    public static void setup() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new GuavaModule());
        final URL resource = Resources.getResource("fixtures/configuration.yaml");
        String configString = Resources.toString(resource, Charset.defaultCharset());
        ringConfiguration = mapper.readValue(configString, RingConfiguration.class);
    }

    @Test
    public void testAddressTranslator() {
        assertThat(ringConfiguration.getAddressTranslatorEnum().getAddressTranslator(), instanceOf(
                EC2MultiRegionAddressTranslator.class));
    }

    @Test
    public void testLoadBalancingPolicies() {
        LoadBalancingPolicyConfiguration loadBalancingPolicyConfiguration =
                ringConfiguration.getLoadBalancingPolicyConfiguration();

        final List<LoadBalancingPolicyEnum> policies = ringConfiguration
                .getLoadBalancingPolicyConfiguration().getLoadBalancingPolicies();
        LoadBalancingPolicy dcAwarePolicy = policies.get(0)
                .getLoadBalancingPolicy(null, loadBalancingPolicyConfiguration);
        assertThat(dcAwarePolicy, instanceOf(DCAwareRoundRobinPolicy.class));
    }

    @Test
    public void testCypherSuites() {
        String[] expected = {"foo", "bar"};
        assertArrayEquals(expected, ringConfiguration.getSecurityConfiguration().getCypherSuites());
    }
}
