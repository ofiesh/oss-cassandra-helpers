package com.clearcapital.oss.cassandra.replication_strategies;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = "class")
public interface ReplicationStrategy {

    public String toCqlString();
}
