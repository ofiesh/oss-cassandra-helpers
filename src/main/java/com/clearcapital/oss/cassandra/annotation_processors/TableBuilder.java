package com.clearcapital.oss.cassandra.annotation_processors;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.CQLHelpers;
import com.clearcapital.oss.cassandra.ColumnDefinition;
import com.clearcapital.oss.cassandra.TemporaryTable;
import com.clearcapital.oss.cassandra.annotations.AdditionalIndex;
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

    private static Logger log = LoggerFactory.getLogger(TableBuilder.class);

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

        AdditionalIndex[] additionalIndexes = annotation.additionalIndexes();

        Collection<ColumnDefinition> columnDefinitions = CassandraTableProcessor.getColumnDefinitionList(annotation);
        if (columnDefinitions == null) {
            return;
        }

        Create create = SchemaBuilder.createTable(tableName);
        addColumnDefinitions(create, columnDefinitions);
        Options createWithOptions = create.withOptions();
        boolean hasOptions = TablePropertiesProcessor.encodeTableProperties(createWithOptions, annotation.properties());
        hasOptions |= addClusteringOrders(createWithOptions, columnDefinitions);

        if (hasOptions) {
            executor.addCommand(CassandraCommand.builder(getSession()).setStatement(createWithOptions).build());
        } else {
            executor.addCommand(CassandraCommand.builder(getSession()).setStatement(create).build());
        }

        for (AdditionalIndex additionalIndex : additionalIndexes) {
            executor.addCommand(CassandraCommand.builder(getSession())
                    .setStatement(SchemaBuilder.createIndex(additionalIndex.name()).onTable(tableName)
                            .andColumn(additionalIndex.columnName()))
                    .build());
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

            executor.addCommand(new SolrCoreWaiter(manager,tableClass,solrOptions.coreCreationTimeoutMs()));
        }
    }

    public TemporaryTable buildTemp() throws AssertException, CassandraException, CommandExecutionException,
            ClientProtocolException, IOException {
        build();
        return new TemporaryTable(getSession(), tableName);
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
                // This allows creating columns with keywords for names, like "password".
                String columnName = getEscapedColumnName(definition);

                switch (definition.getColumnOption()) {
                    case PARTITION_KEY:
                        log.trace("Adding partition key column:" + columnName);
                        create.addPartitionKey(columnName, definition.getDataType());
                        break;
                    case CLUSTERING_KEY_ASC:
                    case CLUSTERING_KEY_DESC:
                        log.trace("Adding clustering column:" + columnName);
                        create.addClusteringColumn(columnName, definition.getDataType());
                        break;
                    case STATIC:
                        log.trace("Adding static column:" + columnName);
                        create.addStaticColumn(columnName, definition.getDataType());
                        break;
                    case NULL:
                        log.trace("Adding column:" + columnName);
                        create.addColumn(columnName, definition.getDataType());
                        break;
                }
            } else {
                create.addColumn(definition.getColumnName(), definition.getDataType());
            }

        }
    }

    private static String getEscapedColumnName(ColumnDefinition definition) {
        String columnName = definition.getColumnName();
        if (columnName.equals("password")) {
            columnName = "\"" + CQLHelpers.escapeCqlString(columnName) + "\"";
        }
        return columnName;
    }

    private static boolean addClusteringOrders(Options options, final Collection<ColumnDefinition> columnDefinitions) {
        boolean result = false;
        for (ColumnDefinition definition : columnDefinitions) {
            if (definition.getColumnOption() != null) {
                String columnName = getEscapedColumnName(definition);
                switch (definition.getColumnOption()) {
                    case CLUSTERING_KEY_ASC:
                        options.clusteringOrder(columnName, SchemaBuilder.Direction.ASC);
                        result = true;
                        break;
                    case CLUSTERING_KEY_DESC:
                        options.clusteringOrder(columnName, SchemaBuilder.Direction.DESC);
                        result = true;
                        break;
                    case NULL:
                    case PARTITION_KEY:
                    case STATIC:
                        // It might seem weird to have all of the options listed here, rather
                        // then just a default: branch. We do this so that, on the off chance that the
                        // enum is expanded later, the compiler will warn us that we need to reconsider
                        // this method.
                        break;
                }
            }
        }
        return result;
    }

}