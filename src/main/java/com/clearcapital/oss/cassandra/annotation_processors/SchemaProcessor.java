package com.clearcapital.oss.cassandra.annotation_processors;

import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.executors.CommandExecutor;

public class SchemaProcessor {

    protected CommandExecutor executor;
    protected MultiRingClientManager manager;

    SchemaProcessor(CommandExecutor executor, MultiRingClientManager manager) {
        this.executor = executor;
        this.manager = manager;
    }
}
