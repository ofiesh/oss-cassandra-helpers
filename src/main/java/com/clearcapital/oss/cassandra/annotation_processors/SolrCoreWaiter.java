package com.clearcapital.oss.cassandra.annotation_processors;

import java.time.*;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.DebuggableCommand;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.java.StackHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.TableMetadata;

/**
 * Waits for solr core to appear.
 */
public class SolrCoreWaiter implements DebuggableCommand {

    private String location;
    private MultiRingClientManager manager;
    private Class<?> tableClass;
    private String tableName;
    private String keyspaceName;
    private long timeout;

    SolrCoreWaiter(MultiRingClientManager manager, Class<?> tableClass, long timeout) throws AssertException {
        location = StackHelpers.getRelativeStackLocation(1);
        this.manager = manager;
        this.tableClass = tableClass;
        CassandraTable annotation = CassandraTableProcessor.getAnnotation(tableClass);
        this.tableName = annotation.tableName();
        this.keyspaceName = manager.getRingClientForGroup(annotation.multiRingGroup()).getPreferredKeyspaceName();
        this.timeout = timeout;
    }

    @Override
    public String getLocation() {
        return location;
    }

    public boolean solrQueryFieldExists() throws AssertException {
        CassandraTable annotation = CassandraTableProcessor.getAnnotation(tableClass);
        String group = annotation.multiRingGroup();
        TableMetadata tableMetadata = manager.getRingClientForGroup(group).getKeyspace(keyspaceName)
                .getTableMetadata(tableName);
        return tableMetadata != null && tableMetadata.getColumn("solr_query") != null;
    }
    
    public SolrCoreWaiter setKeyspaceName(String value) {
        keyspaceName = value;
        return this;
    }

    public SolrCoreWaiter setTableName(String value) {
        tableName = value;
        return this;
    }

    @Override
    public void execute() throws CommandExecutionException {
        try {
            ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC);
            while (!solrQueryFieldExists()) {
                Duration elapsed = Duration.between(start, ZonedDateTime.now(start.getZone()));
                if (elapsed.toMillis() > timeout) {
                    throw new CommandExecutionException("Timed out waiting for solr_query field to appear");
                }
                Thread.sleep(250);
            }
        } catch (AssertException | InterruptedException e) {
            throw new CommandExecutionException(e);
        }
    }

}
