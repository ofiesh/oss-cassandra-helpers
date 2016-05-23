package com.clearcapital.oss.cassandra;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;

import com.clearcapital.oss.cassandra.configuration.LoadBalancingPolicyConfiguration;
import com.clearcapital.oss.cassandra.configuration.RingConfiguration;
import com.clearcapital.oss.cassandra.configuration.SecurityConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.policies.LoadBalancingPolicyEnum;
import com.clearcapital.oss.cassandra.test_support.CassandraTestResource;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.NettySSLOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

public class RingClient {

    private final SSLContextFactory sslContextFactory = new SSLContextFactory();
    private final RingConfiguration configuration;
    private final Cluster cluster;

    public RingClient(RingConfiguration configuration) throws AssertException {
        AssertHelpers.notNull(configuration, "configuration");

        Cluster.Builder builder = new Cluster.Builder();
        for (String host : configuration.getHosts()) {
            builder.addContactPoint(host);
        }
        if (configuration.getPort() != null) {
            builder.withPort(configuration.getPort());
        }
        if (configuration.getAddressTranslatorEnum() != null) {
            builder.withAddressTranslator(configuration.getAddressTranslatorEnum().getAddressTranslator());
        }

        LoadBalancingPolicyConfiguration loadBalancingPolicyConfiguration =
                configuration.getLoadBalancingPolicyConfiguration();
        if (loadBalancingPolicyConfiguration == null && !CollectionUtils.isNotEmpty(
                loadBalancingPolicyConfiguration.getLoadBalancingPolicies())) {

            LoadBalancingPolicy loadBalancingPolicy = null;
            for (LoadBalancingPolicyEnum loadBalancingPolicyEnum : loadBalancingPolicyConfiguration
                    .getLoadBalancingPolicies()) {
                loadBalancingPolicy = loadBalancingPolicyEnum.getLoadBalancingPolicy(loadBalancingPolicy,
                        configuration.getLoadBalancingPolicyConfiguration());
            }
            if(loadBalancingPolicy != null) {
                builder.withLoadBalancingPolicy(loadBalancingPolicy);
            }
        }

        SecurityConfiguration securityConfiguration = configuration.getSecurityConfiguration();
        if (securityConfiguration != null) {
            if( StringUtils.isNotEmpty(securityConfiguration.getUsername()) && StringUtils.isNotEmpty(
                    securityConfiguration.getPassword())) {
                builder.withCredentials(securityConfiguration.getUsername(), securityConfiguration.getPassword());
            }

            if(securityConfiguration.isEnableEncryption()) {
                final SSLContext sslContext = sslContextFactory.createSSLContext(securityConfiguration);

                if (sslContext != null) {
                    SSLOptions sslOptions = JdkSSLOptions.builder().withSSLContext(sslContext)
                            .withCipherSuites(securityConfiguration.getCypherSuites()).build();
                    builder.withSSL(sslOptions);
                }
            }
        }

        this.configuration = configuration;
        this.cluster = builder.build();
    }

    public SessionHelper getSession() {
        return new SessionHelper(cluster.connect(), configuration);
    }

    public SessionHelper getPreferredKeyspace() throws AssertException {
        return getKeyspace(getPreferredKeyspaceName());
    }

    public SessionHelper createPreferredKeyspace() throws AssertException, CassandraException {
        return createKeyspace(getPreferredKeyspaceName());
    }

    public SessionHelper createKeyspace(String keyspaceName) throws CassandraException {
        getSession().createKeyspace(keyspaceName);
        return getKeyspace(keyspaceName);
    }

    public String getPreferredKeyspaceName() throws AssertException {
        AssertHelpers.notNull(configuration.getPreferredKeyspace(), "configuration.preferredKeyspace");

        return configuration.getPreferredKeyspace();
    }

    public SessionHelper getKeyspace(String keyspaceName) {
        return new SessionHelper(cluster.connect(keyspaceName), configuration);
    }

    public KeyspaceMetadata getKeyspaceInfo(String keyspaceName) {
        Metadata clusterMetadata = cluster.getMetadata();
        if (clusterMetadata == null) {
            return null;
        }
        return clusterMetadata.getKeyspace(keyspaceName);
    }

    public boolean keyspaceExists(String keyspaceName) {
        return null != getKeyspaceInfo(keyspaceName);
    }

    /**
     * 
     */
    public TemporaryKeyspace createTemporaryKeyspace(String keyspacePrefix)
            throws AssertException, CassandraException, InterruptedException {
        while (true) {
            synchronized (CassandraTestResource.class) {
                String keyspaceName = CQLHelpers.getUniqueName("tmp_");
                if (!keyspaceExists(keyspaceName)) {
                    CassandraTestResource.log.debug("Creating unique keyspace session:" + keyspaceName);
                    getSession().createKeyspace(keyspaceName);
                    return new TemporaryKeyspace(getKeyspace(keyspaceName));
                }
            }
            Thread.sleep(1);
        }
    }

    public void dropKeyspace(String keyspaceName) {
        getSession().dropKeyspace(keyspaceName);
    }

    public void disconnect() {
        cluster.close();
    }

    public Cluster getCluster() {
        return cluster;
    }

    public ProtocolVersion getProtocolVersion() {
        return getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    }

}
