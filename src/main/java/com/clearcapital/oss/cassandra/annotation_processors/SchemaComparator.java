package com.clearcapital.oss.cassandra.annotation_processors;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.RingClient;
import com.clearcapital.oss.cassandra.SessionHelper;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.configuration.AutoSchemaConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SchemaComparator extends SchemaProcessor {

    private static Logger log = LoggerFactory.getLogger(SchemaComparator.class);

    private AutoSchemaConfiguration autoSchemaConfig;

    public SchemaComparator(CommandExecutor executor, MultiRingClientManager manager,
            AutoSchemaConfiguration autoSchemaConfig) {
        super(executor, manager);
        this.autoSchemaConfig = autoSchemaConfig;
    }

    public void compare() throws ClientProtocolException, AssertException, CassandraException,
            CommandExecutionException, IOException {
    	if (autoSchemaConfig.getKillFirst() && !autoSchemaConfig.getDryRun()) {
    		log.debug("Killing preexisting schemas");
    		
            ImmutableMap<String, RingClient> ringClients = manager.getRingClients();
            for (Entry<String, RingClient> ringClient : ringClients.entrySet()) {
            	String keyspaceName = ringClient.getValue().getPreferredKeyspaceName();
            	ringClient.getValue().getSession().dropKeyspace(keyspaceName);
            }    		
    	}
        ImmutableSet<String> tablesProcessed = compareAnnotatedClasses();
        dropSuperfluousTables(tablesProcessed);
    }

    private void dropSuperfluousTables(ImmutableSet<String> tablesProcessed)
            throws AssertException, CassandraException {
        // check to see if we can dump any extra tables.
        ImmutableSet<String> dropTables = autoSchemaConfig.getDropTables();
        if (dropTables == null) {
            dropTables = ImmutableSet.<String> of();
        }

        ImmutableMap<String, RingClient> ringClients = manager.getRingClients();
        for (Entry<String, RingClient> ringClient : ringClients.entrySet()) {
            log.debug("Checking to see if ring \"" + ringClient.getKey() + "\" has any superfluous tables.");
            SessionHelper session = ringClient.getValue().getPreferredKeyspace();
            KeyspaceMetadata metadata = session.getKeyspaceInfo();
            Collection<TableMetadata> tableMetadatas = metadata.getTables();

            String keyspaceName = metadata.getName();

            for (TableMetadata tableMetadata : tableMetadatas) {
                String tableName = tableMetadata.getName();
                String fullTableName = keyspaceName + "." + tableName;
                if (!tablesProcessed.contains(fullTableName)) {
                    if (dropTables.contains(fullTableName)) {
                        log.debug("Dropping superfluous table:" + tableName);
                        if (!autoSchemaConfig.getDryRun()) {
                            session.dropTableIfExists(tableName);
                        }
                    } else {
                        log.debug("Found superfluous table.  To drop it, run auto-schema --drop-tables '"
                                + fullTableName
                                + "'");
                    }
                }
            }

        }
    }

    private ImmutableSet<String> compareAnnotatedClasses()
            throws AssertException, CassandraException, CommandExecutionException,
            ClientProtocolException, IOException {
        Set<Class<?>> tableClasses = ReflectionHelpers.getTypesAnnotatedWith("/", CassandraTable.class);

        // Check to make sure all tables are up to date.
        log.debug("Comparing schema for table classes");
        ImmutableSet.Builder<String> processedTablesBuilder = ImmutableSet.<String> builder();
        for (Class<?> tableClass : tableClasses) {
            String tableName = CassandraTableProcessor.tableComparator(executor, manager, tableClass)
                    .withConfig(autoSchemaConfig).compare().getFullTableName();
            processedTablesBuilder.add(tableName);
        }
        return processedTablesBuilder.build();
    }

}
