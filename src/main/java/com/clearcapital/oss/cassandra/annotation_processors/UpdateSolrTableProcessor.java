package com.clearcapital.oss.cassandra.annotation_processors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.annotations.SolrTableConfigFile;
import com.clearcapital.oss.cassandra.configuration.WithMultiRingConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.executors.QueuedCommandExecutor;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class UpdateSolrTableProcessor {

    private static Logger log = LoggerFactory.getLogger(UpdateSolrTableProcessor.class);

    public static enum IndexType {
        full,
        inplace,
        none,
        create;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private boolean listSolrTables;
        private String tableName;
        private boolean updateSchema;
        private Boolean updateConfig;
        private IndexType indexType;

        public void execute(WithMultiRingConfiguration configuration)
                throws AssertException, CommandExecutionException, CassandraException {
            if (listSolrTables) {
                executeListSolrTables();
                return;
            } else {
                executeSolrUpdate(configuration);
            }
        }

        private void executeListSolrTables() {
            Iterable<Class<?>> classes = CassandraTableProcessor.getSolrTableClasses();
            log.info("=== Listing available solr tables ===");
            for (Class<?> clazz : classes) {
                log.info("* " + clazz.getName());
            }
        }

        private void executeSolrUpdate(WithMultiRingConfiguration configuration)
                throws AssertException, CommandExecutionException, CassandraException {
            MultiRingClientManager multiRingClientManager = new MultiRingClientManager(
                    configuration.getMultiRingConfiguration());

            Class<?> solrTable = Iterables.find(CassandraTableProcessor.getSolrTableClasses(),
                    new Predicate<Class<?>>() {

                        @Override
                        public boolean apply(Class<?> input) {
                            return input.getName().equals(tableName) || input.getName().endsWith("." + tableName);
                        }
                    });

            AssertHelpers.notNull(solrTable, "solrTable");
            CassandraTable annotation = CassandraTableProcessor.getAnnotation(solrTable);
            AssertHelpers.isTrue(annotation.solrOptions().enabled(), "annotation.solrOptions.enabled");

            try (CommandExecutor executor = new QueuedCommandExecutor()) {
                String multiRingGroup = annotation.multiRingGroup();

                if (updateConfig) {
                    SolrOptionsProcessor.solrFileUploader(executor, multiRingClientManager, solrTable)
                            .setMultiRingGroup(multiRingGroup).setTableName(annotation.tableName())
                            .setSourceName(annotation.solrOptions().solrconfigResourceName())
                            .setDestName("solrconfig.xml").upload();
                }

                if (updateSchema) {
                    SolrOptionsProcessor.solrFileUploader(executor, multiRingClientManager, solrTable)
                            .setMultiRingGroup(multiRingGroup).setTableName(annotation.tableName())
                            .setSourceName(annotation.solrOptions().schemaResourceName()).setDestName("schema.xml")
                            .upload();

                    for (SolrTableConfigFile configFile : annotation.solrOptions().additionalSchemaFiles()) {
                        AssertHelpers.isFalse(StringUtils.isBlank(configFile.resourceFileName()),
                                "configFile.resourceFileName should not be blank");
                        AssertHelpers.isFalse(StringUtils.isBlank(configFile.solrFileName()),
                                "configFile.solrFileName should not be blank");

                        SolrOptionsProcessor.solrFileUploader(executor, multiRingClientManager, solrTable)
                                .setMultiRingGroup(multiRingGroup).setTableName(annotation.tableName())
                                .setSourceName(configFile.resourceFileName()).setDestName(configFile.solrFileName())
                                .upload();
                    }
                }

                switch (indexType) {
                    case create:
                        SolrOptionsProcessor.coreCreator(executor, multiRingClientManager, solrTable)
                                .setMultiRingGroup(multiRingGroup).setTableName(annotation.tableName()).create();
                        break;
                    case full:
                        SolrOptionsProcessor.coreCreator(executor, multiRingClientManager, solrTable)
                                .setMultiRingGroup(multiRingGroup).setTableName(annotation.tableName())
                                .reloadDropIndex();
                        break;
                    case inplace:
                        SolrOptionsProcessor.coreCreator(executor, multiRingClientManager, solrTable)
                                .setMultiRingGroup(multiRingGroup).setTableName(annotation.tableName()).reloadInPlace();
                        break;
                    case none:
                        SolrOptionsProcessor.coreCreator(executor, multiRingClientManager, solrTable)
                                .setMultiRingGroup(multiRingGroup).setTableName(annotation.tableName())
                                .reloadDontReindex();
                        break;
                }

                executor.execute();
            }
        }

        public Builder setListSolrTables(boolean value) {
            listSolrTables = value;
            return this;
        }

        public Builder setTableName(String value) {
            tableName = value;
            return this;
        }

        public Builder setUpdateSchema(boolean value) {
            updateSchema = value;
            return this;
        }

        public Builder setUpdateConfig(Boolean value) {
            updateConfig = value;
            return this;
        }

        public Builder setIndexType(IndexType value) {
            indexType = value;
            return this;
        }

    }
}
