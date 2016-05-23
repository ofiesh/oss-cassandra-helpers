package com.clearcapital.oss.cassandra.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Provides security configuration for the DSE Driver to connect using encryption and username/password auth
 *
 * To use encryption {@link SecurityConfiguration#enableEncryption} must be set to true. If it is set to true,
 * {@link SecurityConfiguration#keyStorePath}, {@link SecurityConfiguration#keyStorePassword},
 * {@link SecurityConfiguration#trustStorePath} and {@link SecurityConfiguration#trustStorePassword} must not be empty.
 *
 * {@link SecurityConfiguration#cypherSuites} defaults to { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" }
 */
public class SecurityConfiguration {

    @JsonProperty
    private boolean enableEncryption = false;

    @JsonProperty
    private String keyStorePath;

    @JsonProperty
    private String keyStorePassword;

    @JsonProperty
    private String trustStorePath;

    @JsonProperty
    private String trustStorePassword;

    @JsonProperty
    private String username;

    @JsonProperty
    private String password;

    @JsonProperty
    private String[] cypherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };

    public boolean isEnableEncryption() {
        return enableEncryption;
    }

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getTrustStorePath() {
        return trustStorePath;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String[] getCypherSuites() {
        return cypherSuites;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final SecurityConfiguration that = (SecurityConfiguration) o;
        return enableEncryption == that.enableEncryption &&
                Objects.equal(keyStorePath, that.keyStorePath) &&
                Objects.equal(keyStorePassword, that.keyStorePassword) &&
                Objects.equal(trustStorePath, that.trustStorePath) &&
                Objects.equal(trustStorePassword, that.trustStorePassword) &&
                Objects.equal(username, that.username) &&
                Objects.equal(password, that.password) &&
                Objects.equal(cypherSuites, that.cypherSuites);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(enableEncryption, keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                username, password, cypherSuites);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("enableEncryption", enableEncryption).add("keyStorePath",
                keyStorePath).add("keyStorePassword", keyStorePassword).add("trustStorePath", trustStorePath).add(
                "trustStorePassword", trustStorePassword).add("username", username).add("password", password).add(
                "cypherSuites", cypherSuites).toString();
    }

    public Builder builder() {
        return new Builder();
    }

    public class Builder {

        private SecurityConfiguration result = new SecurityConfiguration();

        public Builder() {
            result = new SecurityConfiguration();
        }

        public SecurityConfiguration build() {
            return result;
        }

        public Builder setEnableEncryption(boolean enableEncryption) {
            result.enableEncryption = enableEncryption;
            return this;
        }

        public Builder setKeyStorePath(String keyStorePath) {
            result.keyStorePath = keyStorePath;
            return this;
        }

        public Builder setKeyStorePassword(String keyStorePassword) {
            result.keyStorePassword = keyStorePassword;
            return this;
        }

        public Builder setTrustStorePath(String trustStorePath) {
            result.trustStorePath = trustStorePath;
            return this;
        }

        public Builder setTrustStorePassword(String trustStorePassword) {
            result.trustStorePassword = trustStorePassword;
            return this;
        }

        public Builder setCypherSuites(String[] cypherSuites) {
            result.cypherSuites = cypherSuites;
            return this;
        }

        public Builder setUsername(String username) {
            result.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            result.password = password;
            return this;
        }
    }
}
