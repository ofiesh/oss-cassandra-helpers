package com.clearcapital.oss.cassandra.replication_strategies;

public class SimpleStrategy implements ReplicationStrategy {

    Integer replication_factor;

    public Integer getReplication_factor() {
        return replication_factor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        SimpleStrategy result;

        Builder() {
            result = new SimpleStrategy();
        }

        public SimpleStrategy build() {
            return result;
        }

        public Builder setReplication_factor(Integer replication_factor) {
            result.replication_factor = replication_factor;
            return this;
        }
    }

    @Override
    public String toCqlString() {
        return "{ 'class' : 'SimpleStrategy' , 'replication_factor' : " + replication_factor + "}";
    }

}
