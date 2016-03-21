package com.clearcapital.oss.cassandra;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.clearcapital.oss.cassandra.configuration.MultiRingConfiguration;
import com.clearcapital.oss.cassandra.configuration.RingConfiguration;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class MultiRingMapperTest {

    private static final String UNKNOWN_GROUP = "unknownGroup";
    private static final String GROUP1 = "group1";
    private static final String GROUP2 = "group2";
    private static final String HOST1 = "host1";
    private static final String HOST2 = "host2";
    private static final String RING1 = "ring1";
    private static final String RING2 = "ring2";

    @Test
    public void testGroupToRingMapping() throws Exception {
        
        // @formatter:off
        MultiRingConfiguration config = MultiRingConfiguration.builder()
                .setRings(ImmutableMap.<String, RingConfiguration> of(
                        RING1, RingConfiguration.builder()
                            .setHosts(ImmutableList.<String> of (HOST1))
                            .build(),
                        RING2, RingConfiguration.builder()
                            .setHosts(ImmutableList.<String> of (HOST2))
                            .build() ))
                .setDefaultRing(RING1)
                .setGroups(ImmutableMap.<String,String> of(
                        GROUP1,RING1,
                        GROUP2,RING2))
                .build();
        // @formatter:on

        MultiRingClientMapper mapper = new MultiRingClientMapper(config);
        
        assertEquals(RING1,mapper.getConnectionKeyForGroup(GROUP1));
        assertEquals(RING2,mapper.getConnectionKeyForGroup(GROUP2));
        assertEquals(RING1,mapper.getConnectionKeyForGroup(UNKNOWN_GROUP));
    }

}
