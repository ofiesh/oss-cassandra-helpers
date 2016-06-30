package com.clearcapital.oss.cassandra.configuration;

import java.util.Objects;

import com.clearcapital.oss.cassandra.multiring.MultiRingClientMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

/**
 * MultiRingConfiguration supports the {@link MultiRingClientMapper}'s ability to map from Groups to Rings.
 * 
 * A json representation might look something like this:
 * 
 * <pre>
 *  
 * { "rings" :
 *   { "dseRing" : { "hosts" : [ "dse1.mycluster.com" , "dse2.mycluster.com" , "dse3.mycluster.com" ] },    
 *     "communityRing" : { "hosts" : [ "community1.mycluster.com" , "community2.mycluster.com" , "community3.mycluster.com" ] } },
 *   "defaultRing" : "communityRing",
 *   "tableOverrides" : { "solrTable" : "dseRing" , "sparkTable" : "dseRing" , "needsSupportTable" : "dseRing" } }
 * </pre>
 * 
 * With this configuration,
 * 
 * @author eehlinger
 *
 */
public class MultiRingConfiguration {

    @JsonProperty
    private ImmutableMap<String, RingConfiguration> rings;

    @JsonProperty
    private String defaultRing;

    @JsonProperty
    private ImmutableMap<String, String> groups;

    MultiRingConfiguration() {

    }

    @Override
    public int hashCode() {
        return Objects.hash(this, rings, defaultRing, groups);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MultiRingConfiguration) {
            MultiRingConfiguration that = (MultiRingConfiguration) obj;
            return Objects.equals(rings, that.rings) && Objects.equals(defaultRing, that.defaultRing)
                    && Objects.equals(groups, that.groups);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("rings", "rings").add("defaultRing", "defaultRing")
                .add("ringOverrides", groups).toString();
    }

    public ImmutableMap<String, RingConfiguration> getRings() {
        return rings;
    }

    public String getDefaultRing() {
        return defaultRing;
    }

    public ImmutableMap<String, String> getGroups() {
        return groups;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private MultiRingConfiguration result = new MultiRingConfiguration();

        public MultiRingConfiguration build() {
            return result;
        }

        public Builder setRings(ImmutableMap<String, RingConfiguration> value) {
            result.rings = value;
            return this;
        }

        public Builder setDefaultRing(String value) {
            result.defaultRing = value;
            return this;
        }

        public Builder setGroups(ImmutableMap<String, String> value) {
            result.groups = value;
            return this;
        }

    }
}
