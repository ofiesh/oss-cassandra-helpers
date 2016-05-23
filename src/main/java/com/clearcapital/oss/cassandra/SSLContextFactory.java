package com.clearcapital.oss.cassandra;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.configuration.SecurityConfiguration;

/**
 * Creates SSLContext from {@link SecurityConfiguration}
 *
 * See http://www.datastax.com/dev/blog/accessing-secure-dse-clusters-with-cql-native-protocol
 */
public class SSLContextFactory {

    private Logger log = LoggerFactory.getLogger(SSLContextFactory.class);

    public SSLContext createSSLContext(SecurityConfiguration securityConfiguration) {
        if(securityConfiguration != null) {
            if (securityConfiguration.getKeyStorePath() != null && securityConfiguration.getKeyStorePassword() != null
                    && securityConfiguration.getTrustStorePath() != null
                    && securityConfiguration.getTrustStorePassword() != null) {
               try {
                    FileInputStream trustStoreFis = new FileInputStream(securityConfiguration.getTrustStorePath());
                    FileInputStream keyStoreFis = new FileInputStream(securityConfiguration.getKeyStorePath());
                    SSLContext ctx = SSLContext.getInstance("SSL");

                    KeyStore trustStore = KeyStore.getInstance("JKS");
                    trustStore.load(trustStoreFis, securityConfiguration.getTrustStorePassword().toCharArray());
                    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                            TrustManagerFactory.getDefaultAlgorithm());
                    trustManagerFactory.init(trustStore);

                    KeyStore keyStore = KeyStore.getInstance("JKS");
                    keyStore.load(keyStoreFis, securityConfiguration.getKeyStorePassword().toCharArray());
                    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                            KeyManagerFactory.getDefaultAlgorithm());
                    keyManagerFactory.init(keyStore, securityConfiguration.getKeyStorePassword().toCharArray());

                    ctx.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(),
                            new SecureRandom());

                    return ctx;
                } catch (Exception e) {
                    log.error("Unable to setup SSLContext", e);
                }
            } else {
                log.error("Unable to create SSLContext, keyStorePath, keyStorePassword, trustStorePath and"
                        + "trustStorePassword may not be null");
            }
        }

        return null;
    }
}

