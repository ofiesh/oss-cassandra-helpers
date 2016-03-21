package com.clearcapital.oss.cassandra;

import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;


public class LoadBalancingPolicyHelpers {

    /**
     * The varying LoadBalancingPolicy implementations do not provide custom toString() implementations, which can make
     * logging them for debugging a... challenge.
     * 
     * @param policy
     * @return
     * @throws ReflectionPathException
     */
    public static String policyToString(LoadBalancingPolicy policy) throws ReflectionPathException {
        if (policy instanceof RoundRobinPolicy) {
            return "RoundRobinPolicy";
        } else if (policy instanceof DCAwareRoundRobinPolicy) {
            // We use reflection here bc DCAwareRoundRobinPolicy only marks policy @VisibleForTesting
            return "DCAwareRoundRobinPolicy[" + ReflectionHelpers.getFieldValue(policy, "localDc") + "]";
        } else if (policy instanceof WhiteListPolicy) {
            return "WhiteListPolicy [" + ReflectionHelpers.getFieldValue(policy, "whiteList") + "] ("
                    + policyToString(((WhiteListPolicy) policy).getChildPolicy()) + ")";
        } else if (policy instanceof LatencyAwarePolicy) {
            return "LatencyAwarePolicy (" + policyToString(((LatencyAwarePolicy) policy).getChildPolicy()) + ")";
        } else if (policy instanceof TokenAwarePolicy) {
            return "TokenAwarePolicy (" + policyToString(((TokenAwarePolicy) policy).getChildPolicy()) + ")";
        } else {
            return "Unrecognized policy type:" + policy.getClass().getName();
        }
    }

}
