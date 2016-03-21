package com.clearcapital.oss.cassandra.annotation_processors;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;

import com.clearcapital.oss.cassandra.ColumnDefinition;
import com.clearcapital.oss.cassandra.TempTable;
import com.clearcapital.oss.cassandra.annotations.AdditionalIndex;
import com.clearcapital.oss.cassandra.annotations.ClusteringOrder;
import com.clearcapital.oss.cassandra.annotations.SolrOptions;
import com.clearcapital.oss.cassandra.annotations.SolrTableConfigFile;
import com.clearcapital.oss.cassandra.bundles.CassandraCommand;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

public class TableBuilder extends TableProcessor<TableBuilder> {

    TableBuilder(CommandExecutor executor, MultiRingClientManager manager, final Class<?> tableClass)
            throws AssertException {
        super(executor, manager, tableClass);
        setSelf(this);
    }

    public void build() throws AssertException, CassandraException, CommandExecutionException, ClientProtocolException,
            IOException {
        AssertHelpers.isFalse(StringUtils.isBlank(tableName),
                "tableName cannot be blank during table schema comparison.");

        if (getSession().tableExists(tableName)) {
            return;
        }

        ClusteringOrder[] clusteringOrder = annotation.clusteringOrder();
        AdditionalIndex[] additionalIndexes = annotation.additionalIndexes();

        Collection<ColumnDefinition> columnDefinitions = CassandraTableProcessor.getColumnDefinitionList(annotation);
        if (columnDefinitions == null) {
            return;
        }

        Create create = SchemaBuilder.createTable(tableName);
        addColumnDefinitions(create, columnDefinitions);
        Options createWithOptions = create.withOptions();
        boolean hasOptions = TablePropertiesProcessor.encodeTableProperties(createWithOptions,
                annotation.properties());
        hasOptions |= addClusteringOrders(createWithOptions, clusteringOrder);

        if (hasOptions) {
            executor.addCommand(CassandraCommand.builder(getSession()).setStatement(createWithOptions).build());
        } else {
            executor.addCommand(CassandraCommand.builder(getSession()).setStatement(create).build());
        }

        for (AdditionalIndex additionalIndex : additionalIndexes) {
            if (additionalIndex.indexMapKeys()) {
                executor.addCommand(CassandraCommand.builder(getSession())
                        .setStatement(SchemaBuilder.createIndex(additionalIndex.name()).onTable(tableName)
                                .andKeysOfColumn(additionalIndex.columnName()))
                        .build());
            } else {
                executor.addCommand(CassandraCommand.builder(getSession())
                        .setStatement(SchemaBuilder.createIndex(additionalIndex.name()).onTable(tableName)
                                .andKeysOfColumn(additionalIndex.columnName()))
                        .build());
            }
        }

        SolrOptions solrOptions = annotation.solrOptions();
        if (solrOptions.enabled()) {
            AssertHelpers.isFalse(StringUtils.isBlank(solrOptions.schemaResourceName()),
                    "solrOptions.schemaResourceName should not be blank");
            AssertHelpers.isFalse(StringUtils.isBlank(solrOptions.solrconfigResourceName()),
                    "solrOptions.solrconfigResourceName should not be blank");

            SolrOptionsProcessor.solrFileUploader(executor, manager, tableClass).setMultiRingGroup(multiRingGroup)
                    .setTableName(tableName).setSourceName(annotation.solrOptions().solrconfigResourceName())
                    .setDestName("solrconfig.xml").upload();

            SolrOptionsProcessor.solrFileUploader(executor, manager, tableClass).setMultiRingGroup(multiRingGroup)
                    .setTableName(tableName).setSourceName(annotation.solrOptions().schemaResourceName())
                    .setDestName("schema.xml").upload();

            for (SolrTableConfigFile configFile : annotation.solrOptions().additionalSchemaFiles()) {
                AssertHelpers.isFalse(StringUtils.isBlank(configFile.resourceFileName()),
                        "configFile.resourceFileName should not be blank");
                AssertHelpers.isFalse(StringUtils.isBlank(configFile.solrFileName()),
                        "configFile.solrFileName should not be blank");

                SolrOptionsProcessor.solrFileUploader(executor, manager, tableClass).setMultiRingGroup(multiRingGroup)
                        .setTableName(tableName).setSourceName(configFile.resourceFileName())
                        .setDestName(configFile.solrFileName()).upload();
            }

            SolrOptionsProcessor.coreCreator(executor, manager, tableClass).setMultiRingGroup(multiRingGroup)
                    .setTableName(tableName).create();
        }
        // if (SolrTableProcessor.getSolrAnnotation(tableClass) != null) {
        // SolrTableProcessor.uploadConfig(schema, tableClass);
        // SolrTableProcessor.uploadSchema(schema, tableClass);
        // SolrTableProcessor.createCore(schema, tableClass);
        // }
    }

    public TempTable buildTemp() throws AssertException, CassandraException, CommandExecutionException,
            ClientProtocolException, IOException {
        build();
        return new TempTable(getSession(), tableName);
    }

    private static void addColumnDefinitions(Create create, final Collection<ColumnDefinition> columnDefinitions) {
        if (columnDefinitions == null) {
            return;
        }

        for (ColumnDefinition definition : columnDefinitions) {
            if (definition.getIsCreatedElsewhere()) {
                continue;
            }

            if (definition.getColumnOption() != null) {
                switch (definition.getColumnOption()) {
                    case PARTITION_KEY:
                        create.addPartitionKey(definition.getColumnName(), definition.getDataType());
                        break;
                    case CLUSTERING_KEY:
                        create.addClusteringColumn(definition.getColumnName(), definition.getDataType());
                        break;
                    case STATIC:
                        create.addStaticColumn(definition.getColumnName(), definition.getDataType());
                        break;
                    case NULL:
                        create.addColumn(definition.getColumnName(), definition.getDataType());
                        break;
                }
            } else {
                create.addColumn(definition.getColumnName(), definition.getDataType());
            }

        }
    }

    private static boolean addClusteringOrders(Options options, ClusteringOrder[] clusteringOrder) {
        if (clusteringOrder == null || clusteringOrder.length == 0) {
            return false;
        }
        for (ClusteringOrder definition : clusteringOrder) {
            options.clusteringOrder(definition.columnName(),
                    definition.descending() ? SchemaBuilder.Direction.DESC : SchemaBuilder.Direction.ASC);
        }
        return true;
    }

}