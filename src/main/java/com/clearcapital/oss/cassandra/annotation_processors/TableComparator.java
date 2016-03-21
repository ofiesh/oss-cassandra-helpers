package com.clearcapital.oss.cassandra.annotation_processors;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.CQLHelpers;
import com.clearcapital.oss.cassandra.ColumnDefinition;
import com.clearcapital.oss.cassandra.bundles.CassandraCommand;
import com.clearcapital.oss.cassandra.configuration.AutoSchemaConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;


public class TableComparator extends TableProcessor<TableComparator> {

    private static Logger log = LoggerFactory.getLogger(TableComparator.class);

    private boolean dryRun;
    private ImmutableSet<String> dropColumns;

    TableComparator(CommandExecutor executor, MultiRingClientManager manager, final Class<?> tableClass)
            throws AssertException {
        super(executor, manager, tableClass);
        setSelf(this);
        dryRun = false;
    }

    public TableComparator compare() throws AssertException, CassandraException, CommandExecutionException,
            ClientProtocolException, IOException {
        AssertHelpers.isFalse(StringUtils.isBlank(tableName),
                "tableName cannot be blank during table schema comparison.");

        log.debug("Comparing schema for table:" + tableName);

        // Make sure the table exists
        TableMetadata tableMetadata = getSession().getTableMetadata(tableName);
        if (tableMetadata == null) {
            log.debug("Table does not exist:" + tableName);
            if (!dryRun) {
                CassandraTableProcessor.tableBuilder(executor, manager, tableClass)
                        .setTableName(tableName)
                        .setMultiRingGroup(multiRingGroup).build();
            }
            return this;
        }

        addMissingColumns(tableMetadata);
        removeExtraColumns(tableMetadata);
        return this;
    }

    private void removeExtraColumns(TableMetadata tableMetadata)
            throws CommandExecutionException, AssertException, CassandraException {
        Map<String, ColumnDefinition> columnMap = CassandraTableProcessor.getColumnDefinitionMap(annotation);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnMap.containsKey(columnMetadata.getName())) {
                continue;
            }
            Statement dropColumn = SchemaBuilder.alterTable(tableName).dropColumn(columnMetadata.getName());
            log.debug("Table " + tableName + ": extra column found:" + columnMetadata.getName() + " dropQuery:"
                    + CQLHelpers.getQueryText(dropColumn));

            String safetyString = tableName + "." + columnMetadata.getName();
            if (dropColumns == null || !dropColumns.contains(safetyString)) {
                log.debug(" - NOT DROPPING COLUMN BECAUSE NOT SPECIFICALLY ASKED TO BY PUTTING \"" + safetyString
                        + "\" IN DROPCOLUMNS");
                continue;
            }
            if (dryRun) {
                continue;
            }
            executor.addCommand(CassandraCommand.builder(getSession()).setStatement(dropColumn).build());
        }
    }

    private void addMissingColumns(TableMetadata tableMetadata)
            throws CommandExecutionException, AssertException, CassandraException {
        Collection<ColumnDefinition> columnList = CassandraTableProcessor.getColumnDefinitionList(annotation);
        for (ColumnDefinition columnDefinition : columnList) {
            if (columnDefinition.getIsCreatedElsewhere()) {
                continue;
            }
            String columnName = columnDefinition.getColumnName();
            ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);
            if (columnMetadata == null) {
                log.debug("Table " + tableName + " is missing column " + columnName);
                Statement statement = SchemaBuilder.alterTable(tableName).addColumn(columnName)
                        .type(columnDefinition.getDataType());
                log.debug("CQL:" + CQLHelpers.getQueryText(statement));
                if (!dryRun) {
                    executor.addCommand(CassandraCommand.builder(getSession()).setStatement(statement).build());
                }
            } else {
                // make sure the type is correct...
                if (!Objects.equal(columnMetadata.getType(), columnDefinition.getDataType())) {
                    log.debug("Column: " + columnName + " Expected Column type:" + columnDefinition.getDataType()
                            + " Actual Column type:" + columnMetadata.getType());
                    
                    Statement query = SchemaBuilder.alterTable(tableName).alterColumn(columnName)
                            .type(columnDefinition.getDataType());
                    log.info("CQL:" + CQLHelpers.getQueryText(query));
                    if (!dryRun) {
                        executor.addCommand(CassandraCommand.builder(getSession()).setStatement(query).build());
                    }
                } else {
                    log.trace("Table " + tableName + ": cassandra matched field " + columnMetadata.getName());
                }
            }
        }
    }

    public TableComparator setDryRun(boolean value) {
        dryRun = value;
        return this;
    }

    public TableComparator setDropColumns(ImmutableSet<String> value) {
        dropColumns = value;
        return this;
    }

    public TableComparator withConfig(AutoSchemaConfiguration autoSchemaConfig) {
        setDryRun(autoSchemaConfig.getDryRun());
        setDropColumns(autoSchemaConfig.getDropColumns());
        return this;
    }

}