package com.clearcapital.oss.cassandra.policies;

import java.util.concurrent.TimeUnit;

import com.clearcapital.oss.cassandra.configuration.LoadBalancingPolicyConfiguration;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/**
 * Provides serializable access points to {@link LoadBalancingPolicy}
 */
public enum LoadBalancingPolicyEnum {

    RoundRobinPolicy((p, c) -> new RoundRobinPolicy()),
    DCAwareRoundRobinPolicy((p, c) ->  new DCAwareRoundRobinPolicy.Builder().withLocalDc(c.getLocalDataCenterName())
            .build()),
    WhiteListPolicy(new WhiteListPolicyFactory()),
    TokenAwarePolicy((p, c) -> new TokenAwarePolicy(p)),
    LatencyAwarePolicy((p, c) -> new LatencyAwarePolicy.Builder(p).withRetryPeriod(
            c.getRetryPeriodSeconds() == null ? 60 : c.getRetryPeriodSeconds(), TimeUnit.SECONDS).build());

    private final LoadBalancingPolicyFactory loadBalancingPolicyFactory;

    LoadBalancingPolicyEnum(LoadBalancingPolicyFactory loadBalancingPolicyFactory) {
        this.loadBalancingPolicyFactory = loadBalancingPolicyFactory;
    }

    /**
     * Creates a LoadBalancingPolicy for the specific type of LoadBalancingPolicyEnum
     * @param childLoadBalancingPolicy If supported by the policy, specifies a child policy to fall back on
     * @param loadBalancingPolicyConfiguration Policy specific configuration detail to create the policy
     * @return return the {@link LoadBalancingPolicy instance associated with the {@link LoadBalancingPolicyEnum}}
     */
    public LoadBalancingPolicy getLoadBalancingPolicy(LoadBalancingPolicy childLoadBalancingPolicy,
            LoadBalancingPolicyConfiguration loadBalancingPolicyConfiguration) {
        return loadBalancingPolicyFactory.createLoadBalancingPolicy(childLoadBalancingPolicy,
                loadBalancingPolicyConfiguration);
    }
}
