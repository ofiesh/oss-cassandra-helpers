package com.clearcapital.oss.cassandra.policies;

import com.clearcapital.oss.cassandra.configuration.LoadBalancingPolicyConfiguration;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

public interface LoadBalancingPolicyFactory {

    LoadBalancingPolicy createLoadBalancingPolicy(LoadBalancingPolicy childLoadBalancingPolicy,
            LoadBalancingPolicyConfiguration loadBalancingPolicyConfiguration);
}
