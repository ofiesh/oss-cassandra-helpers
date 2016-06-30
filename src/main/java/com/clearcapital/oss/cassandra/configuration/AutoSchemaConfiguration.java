package com.clearcapital.oss.cassandra.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

/**
 * Configure how Auto Schema support will work.
 * 
 * @author eehlinger
 */
public class AutoSchemaConfiguration {

    private Boolean dryRun;
    private ImmutableSet<String> dropColumns;
    private ImmutableSet<String> dropTables;
    public Boolean killFirst;

    @JsonProperty
    public Boolean getDryRun() {
        return dryRun;
    }

    @JsonProperty
    public Boolean getKillFirst() {
        return killFirst;
    }

    @JsonProperty
    public ImmutableSet<String> getDropColumns() {
        return dropColumns;
    }

    @JsonProperty
    public ImmutableSet<String> getDropTables() {
        return dropTables;
    }

    public static class Builder {

        private final AutoSchemaConfiguration result;

        Builder() {
            result = new AutoSchemaConfiguration();
        }

        public AutoSchemaConfiguration build() {
            return result;
        }

        public Builder setDryRun(Boolean value) {
            result.dryRun = value;
            return this;
        }

        public Builder setDropColumns(ImmutableSet<String> value) {
            result.dropColumns = value;
            return this;
        }

        public Builder setDropTables(ImmutableSet<String> value) {
            result.dropTables = value;
            return this;
        }

        public Builder setKillFirst(Boolean value) {
            result.killFirst = value;
            return this;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

}
