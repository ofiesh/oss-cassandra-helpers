package com.clearcapital.oss.cassandra.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

/**
 * Provide configuration to tell the DSE Cassandra driver how to connect to a ring.
 * 
 * @author eehlinger
 *
 */
public class RingConfiguration {

    @JsonProperty
    private ImmutableList<String> hosts;

    @JsonProperty
    private Integer port;

    @JsonProperty
    private String preferredKeyspace;

    @JsonProperty
    private String solrUri;

    @Override
    public int hashCode() {
        return Objects.hashCode(this, hosts, port, preferredKeyspace, solrUri);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RingConfiguration) {
            RingConfiguration that = (RingConfiguration) obj;
            return Objects.equal(hosts, that.hosts) && Objects.equal(port, that.port)
                    && Objects.equal(preferredKeyspace, that.preferredKeyspace) && Objects.equal(solrUri, that.solrUri);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("hosts", hosts).add("port", port)
                .add("preferredKeyspace", preferredKeyspace).add("solrUri", solrUri).toString();
    }

    public ImmutableList<String> getHosts() {
        return hosts;
    }

    public Integer getPort() {
        return port;
    }

    public String getPreferredKeyspace() {
        return preferredKeyspace;
    }

    public String getSolrUri() {
        return solrUri;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        RingConfiguration result;

        Builder() {
            result = new RingConfiguration();
        }

        public RingConfiguration build() {
            return result;
        }

        public Builder setHosts(ImmutableList<String> value) {
            result.hosts = value;
            return this;
        }

        public Builder setPort(Integer value) {
            result.port = value;
            return this;
        }

        public Builder setKeyspace(String value) {
            result.preferredKeyspace = value;
            return this;
        }

        public Builder setSolrUri(String solrUri) {
            result.solrUri = solrUri;
            return this;
        }
    }

}
