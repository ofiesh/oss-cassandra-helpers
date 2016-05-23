package com.clearcapital.oss.cassandra.configuration;

import java.util.Collection;
import java.util.List;

import com.clearcapital.oss.cassandra.policies.LoadBalancingPolicyEnum;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Provides configuration for DSE Driver load balancing
 *
 * For available load balancing policies see {@link LoadBalancingPolicyEnum}
 *
 * For {@link LoadBalancingPolicyEnum#DCAwareRoundRobinPolicy} set the {@link Builder#setLocalDataCenterName(String)}
 *
 * For {@link LoadBalancingPolicyEnum#WhiteListPolicy} set the {@link Builder#setWhiteListHosts(Collection)}
 *
 * for {@link LoadBalancingPolicyEnum#LatencyAwarePolicy} set the {@link Builder#retryPeriodSeconds}
 */
public class LoadBalancingPolicyConfiguration {

    @JsonProperty
    private List<LoadBalancingPolicyEnum> loadBalancingPolicies;
    @JsonProperty
    private String localDataCenterName;
    @JsonProperty
    private Collection<String> whiteListHosts;
    @JsonProperty
    private Integer retryPeriodSeconds;

    public List<LoadBalancingPolicyEnum> getLoadBalancingPolicies() {
        return loadBalancingPolicies;
    }

    public String getLocalDataCenterName() {
        return localDataCenterName;
    }

    public Collection<String> getWhiteListHosts() {
        return whiteListHosts;
    }

    public Integer getRetryPeriodSeconds() {
        return retryPeriodSeconds;
    }

    public Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final LoadBalancingPolicyConfiguration that = (LoadBalancingPolicyConfiguration) o;
        return Objects.equal(loadBalancingPolicies, that.loadBalancingPolicies) &&
                Objects.equal(localDataCenterName, that.localDataCenterName) &&
                Objects.equal(whiteListHosts, that.whiteListHosts) &&
                Objects.equal(retryPeriodSeconds, that.retryPeriodSeconds);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(loadBalancingPolicies, localDataCenterName, whiteListHosts, retryPeriodSeconds);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("loadBalancingPolicies", loadBalancingPolicies).add(
                "localDataCenterName", localDataCenterName).add("whiteListHosts", whiteListHosts).add(
                "retryPeriodSeconds", retryPeriodSeconds).toString();
    }

    public class Builder {

        private LoadBalancingPolicyConfiguration result;

        public Builder() {
            result = new LoadBalancingPolicyConfiguration();
        }

        public LoadBalancingPolicyConfiguration build() {
            return result;
        }

        public Builder setLoadBalancingPolicies(List<LoadBalancingPolicyEnum> loadBalancingPolicies) {
            result.loadBalancingPolicies = loadBalancingPolicies;
            return this;
        }

        public Builder setLocalDataCenterName(String localDataCenterName) {
            result.localDataCenterName = localDataCenterName;
            return this;
        }

        public Builder setWhiteListHosts(Collection<String> whiteListHosts) {
            result.whiteListHosts = whiteListHosts;
            return this;
        }

        public Builder setRetryPeriodSeconds(Integer retryPeriodSeconds) {
            result.retryPeriodSeconds = retryPeriodSeconds;
            return this;
        }
    }
}
