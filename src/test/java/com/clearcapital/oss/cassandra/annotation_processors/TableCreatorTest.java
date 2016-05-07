package com.clearcapital.oss.cassandra.annotation_processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.CQLHelpers;
import com.clearcapital.oss.cassandra.SessionHelper;
import com.clearcapital.oss.cassandra.TemporaryTable;
import com.clearcapital.oss.cassandra.test_support.CassandraTestResource;
import com.clearcapital.oss.executors.ImmediateCommandExecutor;
import com.datastax.driver.core.TableMetadata;

public class TableCreatorTest {

    private static Logger log = LoggerFactory.getLogger(TableCreatorTest.class);

    @ClassRule
    public static CassandraTestResource cassandraResource = new CassandraTestResource(Arrays.asList("cassandra.yaml"));

    @Test
    public void testGetAnnotation() throws Exception {
        assertNotNull(CassandraTableProcessor.getAnnotation(DemoTable.class));
    }

    @Test
    public void testCreateTable() throws Exception {
        ImmediateCommandExecutor executor = new ImmediateCommandExecutor();

        SessionHelper session = null;
        String tableName = CQLHelpers.getUniqueName("tmp_");
        try (TemporaryTable tempTable = CassandraTableProcessor
                .tableBuilder(executor, cassandraResource.getClient(), DemoTable.class).setTableName(tableName)
                .buildTemp()) {

            session = tempTable.getSession();
            assertEquals(tableName, tempTable.getTableName());

            assertTrue(session.tableExists(tempTable.getTableName()));

            TableMetadata metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.ID_COLUMN));
            assertNotNull(metaData.getColumn(DemoTable.UPDATE_ID_COLUMN));
            assertNotNull(metaData.getColumn(DemoTable.FLUID_TYPE_COLUMN));
            assertNotNull(metaData.getColumn(DemoTable.JSON_COLUMN));

        }
        assertFalse(session.tableExists(tableName));
    }

    @Test
    public void testCreateSolrIndexedTable() throws Exception {
        ImmediateCommandExecutor executor = new ImmediateCommandExecutor();

        SessionHelper session = null;
        String tableName = CQLHelpers.getUniqueName("tmp_");
        try (TemporaryTable tempTable = CassandraTableProcessor
                .tableBuilder(executor, cassandraResource.getClient(), DemoSolrTable.class).setTableName(tableName)
                .buildTemp()) {

            session = tempTable.getSession();
            assertEquals(tableName, tempTable.getTableName());

            assertTrue(session.tableExists(tempTable.getTableName()));

            TableMetadata metaData = session.getTableMetadata(tableName);
            int attempt = 0;
            while (true) {
                assertNotNull(metaData.getColumn(DemoSolrTable.ID_COLUMN));
                assertNotNull(metaData.getColumn(DemoSolrTable.JSON_COLUMN));
                if (null != metaData.getColumn(DemoSolrTable.SOLR_QUERY_COLUMN)) {
                    break;
                }
                if (++attempt > 3) {
                    fail("Solr column \"" + DemoSolrTable.SOLR_QUERY_COLUMN + "\" should have appeared by now.");
                }
                // Wait a few seconds between attempts
                log.debug("Giving DSE " + attempt + "/2 seconds to get solr connected.");
                Thread.sleep(attempt * 500);
                metaData = session.getTableMetadata(tableName);
            }

        }
        assertFalse(session.tableExists(tableName));
    }
}
