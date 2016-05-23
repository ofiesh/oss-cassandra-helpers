package com.clearcapital.oss.cassandra.test_support;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collection;

import org.apache.http.client.ClientProtocolException;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.annotation_processors.CassandraTableProcessor;
import com.clearcapital.oss.cassandra.annotation_processors.SolrCoreModifier;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.configuration.MultiRingConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.ImmediateCommandExecutor;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.json.JsonSerializer;
import com.datastax.driver.core.TableMetadata;
import com.thenewentity.utils.dropwizard.MultipleConfigurationMerger;

/**
 * Provide a JUnit-compatible ExternalResource which understand {@link MultiRingClientManager} and supports using
 * Dropwizard-Multi-Config to specify the location of the various CassandraRings which a test may want access to.
 * 
 * @author eehlinger
 *
 */
public class CassandraTestResource extends ExternalResource {

    public static Logger log = LoggerFactory.getLogger(CassandraTestResource.class);

    public final MultiRingClientManager multiRingClientManager;

    public CassandraTestResource(Collection<String> configPaths) {
        multiRingClientManager = buildClient(configPaths);
    }

    private static MultiRingClientManager buildClient(Collection<String> configPaths) {
        try {
            // @formatter:off
            MultipleConfigurationMerger merger = MultipleConfigurationMerger.builder()
                    .setObjectMapper(JsonSerializer.getInstance().getObjectMapper())
                    .build();
            // @formatter:on

            MultiRingConfiguration config = merger.loadConfigs(configPaths, MultiRingConfiguration.class);
            return new MultiRingClientManager(config);
        } catch (AssertException e) {
            log.error("Failed to buildClient", e);
            return null;
        }
    }

    public void recreateTable(Class<?> tableClass) throws AssertException, CassandraException, ClientProtocolException,
            CommandExecutionException, IOException {
        String multiRingGroup = CassandraTableProcessor.getAnnotation(tableClass).multiRingGroup();

        multiRingClientManager.getRingClientForGroup(multiRingGroup).createPreferredKeyspace();
        while (tableExists(multiRingGroup, CassandraTableProcessor.getAnnotation(tableClass).tableName())) {
            CassandraTableProcessor.dropTableIfExists(multiRingClientManager, tableClass);
        }
        CassandraTableProcessor.tableBuilder(new ImmediateCommandExecutor(), multiRingClientManager, tableClass)
                .build();
    }

    public void truncateTable(Class<?> tableClass) throws AssertException {
        CassandraTable annotation = CassandraTableProcessor.getAnnotation(tableClass);
        multiRingClientManager.getRingClientForGroup(annotation.multiRingGroup()).getPreferredKeyspace()
                .truncateTable(annotation.tableName());
    }

    public MultiRingClientManager getClient() {
        return multiRingClientManager;
    }

    @Override
    protected void before() throws Throwable {
        assertNotNull(multiRingClientManager);
    }

    public boolean tableExists(String group, String table) throws AssertException {
        return multiRingClientManager.getRingClientForGroup(group).getPreferredKeyspace().tableExists(table);
    }

    public TableMetadata getTableMetadata(String group, String table) throws AssertException {
        return multiRingClientManager.getRingClientForGroup(group).getPreferredKeyspace().getTableMetadata(table);
    }

    public boolean tableExists(Class<?> tableClass) throws AssertException {
        CassandraTable annotation = CassandraTableProcessor.getAnnotation(tableClass);
        return tableExists(annotation.multiRingGroup(), annotation.tableName());
    }

    public void dropTable(Class<?> tableClass) throws AssertException, CassandraException {
        CassandraTable annotation = CassandraTableProcessor.getAnnotation(tableClass);
        multiRingClientManager.getRingClientForGroup(annotation.multiRingGroup()).getPreferredKeyspace()
                .dropTableIfExists(annotation.tableName());
    }

    public void reloadCoreInPlace(Class<?> tableClass) throws AssertException, CommandExecutionException, CassandraException {
        ImmediateCommandExecutor executor = new ImmediateCommandExecutor();
        new SolrCoreModifier(executor, multiRingClientManager, tableClass).reloadInPlace();
    }

}
