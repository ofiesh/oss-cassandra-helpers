package com.clearcapital.oss.cassandra.multiring;

import com.clearcapital.oss.cassandra.configuration.MultiRingConfiguration;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;

public class MultiRingClientMapper {

    private final MultiRingConfiguration configuration;

    public MultiRingClientMapper(MultiRingConfiguration configuration) throws AssertException {
        AssertHelpers.notNull(configuration, "configuration");
        AssertHelpers.notNull(configuration.getDefaultRing(), "configuration.defaultRing");
        AssertHelpers.notNull(configuration.getRings(), "configuration.rings");
        this.configuration = configuration;
    }

    public String getConnectionKeyForGroup(String groupKey) throws AssertException {
        if (getConfiguration().getGroups() == null) {
            return getConfiguration().getDefaultRing();
        }
        if (!getConfiguration().getGroups().containsKey(groupKey)) {
            return getConfiguration().getDefaultRing();
        }
        return getConfiguration().getGroups().get(groupKey);
    }

    public MultiRingConfiguration getConfiguration() {
        return configuration;
    }

}
