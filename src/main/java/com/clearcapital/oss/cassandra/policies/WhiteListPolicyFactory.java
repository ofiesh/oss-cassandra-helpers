package com.clearcapital.oss.cassandra.policies;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.configuration.LoadBalancingPolicyConfiguration;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

/**
 * Parses a {@link Collection<String>} of hosts and creates a {@link WhiteListPolicy}
 */
public class WhiteListPolicyFactory implements LoadBalancingPolicyFactory {

    private final Logger logger = LoggerFactory.getLogger(WhiteListPolicyFactory.class);

    /**
     * Creates a {@link WhiteListPolicy from the given {@link LoadBalancingPolicyConfiguration}
     * @param childLoadBalancingPolicy 
     * @param loadBalancingPolicyConfiguration
     * @return
     */
    @Override
    public LoadBalancingPolicy createLoadBalancingPolicy(LoadBalancingPolicy childLoadBalancingPolicy,
            final LoadBalancingPolicyConfiguration loadBalancingPolicyConfiguration) {

        Collection<InetSocketAddress> whiteListHosts = new ArrayList<>();
        for(String host : loadBalancingPolicyConfiguration.getWhiteListHosts()) {
            if(host != null) {
                String[] parts = host.split(":", 2);
                String hostName = parts[0];
                Integer port = Integer.valueOf(parts[1]);
                whiteListHosts.add(new InetSocketAddress(hostName, port));
            } else {
                logger.error("null host name specified for white list hosts");
            }
        }

        return new WhiteListPolicy(childLoadBalancingPolicy, whiteListHosts);
    }
}
