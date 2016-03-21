package com.clearcapital.oss.cassandra.annotation_processors;

import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.java.exceptions.AssertException;

public class SolrOptionsProcessor {

    public static SolrFileUploader solrFileUploader(CommandExecutor executor, MultiRingClientManager manager,
            Class<?> tableClass) throws AssertException {
        return new SolrFileUploader(executor, manager, tableClass);
    }

    public static SolrCoreModifier coreCreator(CommandExecutor executor, MultiRingClientManager manager,
            Class<?> tableClass) throws AssertException {
        return new SolrCoreModifier(executor, manager, tableClass);
    }

}