package com.clearcapital.oss.cassandra.annotation_processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.ClassRule;
import org.junit.Test;

import com.clearcapital.oss.cassandra.SessionHelper;
import com.clearcapital.oss.cassandra.TempTable;
import com.clearcapital.oss.cassandra.test_support.CassandraTestResource;
import com.clearcapital.oss.executors.ImmediateCommandExecutor;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableSet;

public class TableComparatorTest {

    @ClassRule
    public static CassandraTestResource cassandraResource = new CassandraTestResource(Arrays.asList("cassandra.yaml"));

    @Test
    public void testTableComparator_AddMissingColumn() throws Exception {
        ImmediateCommandExecutor executor = new ImmediateCommandExecutor();

        SessionHelper session = null;
        String tableName = cassandraResource.getUniqueName("tmp_");

        try (TempTable tempTable = CassandraTableProcessor
                .tableBuilder(executor, cassandraResource.getClient(), DemoTable.class).setTableName(tableName)
                .buildTemp()) {

            session = tempTable.getSession();
            assertEquals(tableName, tempTable.getTableName());
            TableMetadata metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.JSON_COLUMN));

            session.execute(SchemaBuilder.alterTable(tableName).dropColumn(DemoTable.JSON_COLUMN));
            metaData = session.getTableMetadata(tableName);
            assertNull(metaData.getColumn(DemoTable.JSON_COLUMN));

            CassandraTableProcessor.tableComparator(executor, cassandraResource.getClient(), DemoTable.class)
                    .setTableName(tableName).compare();

            metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.JSON_COLUMN));
        }
        assertFalse(session.tableExists(tableName));
    }

    @Test
    public void testTableComparator_AddMissingTable() throws Exception {
        ImmediateCommandExecutor executor = new ImmediateCommandExecutor();

        SessionHelper session = null;
        String tableName = cassandraResource.getUniqueName("tmp_");

        try (TempTable tempTable = CassandraTableProcessor
                .tableBuilder(executor, cassandraResource.getClient(), DemoTable.class).setTableName(tableName)
                .buildTemp()) {

            session = tempTable.getSession();
            assertEquals(tableName, tempTable.getTableName());
            TableMetadata metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData);

            session.execute(SchemaBuilder.dropTable(tableName));
            metaData = session.getTableMetadata(tableName);
            assertNull(metaData);

            CassandraTableProcessor.tableComparator(executor, cassandraResource.getClient(), DemoTable.class)
                    .setTableName(tableName).compare();

            metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData);
        }
        assertFalse(session.tableExists(tableName));
    }

    @Test
    public void testTableComparator_ChangeColumnType() throws Exception {
        ImmediateCommandExecutor executor = new ImmediateCommandExecutor();

        SessionHelper session = null;
        String tableName = cassandraResource.getUniqueName("tmp_");

        try (TempTable tempTable = CassandraTableProcessor
                .tableBuilder(executor, cassandraResource.getClient(), DemoTable.class).setTableName(tableName)
                .buildTemp()) {

            // Quick sanity check to make sure the table exists and our fluidType column is correct
            session = tempTable.getSession();
            assertEquals(tableName, tempTable.getTableName());
            TableMetadata metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.FLUID_TYPE_COLUMN));
            assertEquals(DataType.text(), metaData.getColumn(DemoTable.FLUID_TYPE_COLUMN).getType());

            // Let's get in our time machine, and go back to an imaginary timeline where
            // this column was ascii. Note, C* doesn't let you change a column from text->ascii, but it
            // does let you change a column from ascii->text. C* can be a strange beast at times.
            session.execute(SchemaBuilder.alterTable(tableName).dropColumn(DemoTable.FLUID_TYPE_COLUMN));
            session.execute(SchemaBuilder.alterTable(tableName).addColumn(DemoTable.FLUID_TYPE_COLUMN)
                    .type(DataType.ascii()));
            metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.FLUID_TYPE_COLUMN));
            assertEquals(DataType.ascii(), metaData.getColumn(DemoTable.FLUID_TYPE_COLUMN).getType());

            // Now we compare against our schema, and verify that CassandraTableProcessor recognizes and fixes the
            // discrepancy.
            CassandraTableProcessor.tableComparator(executor, cassandraResource.getClient(), DemoTable.class)
                    .setTableName(tableName).compare();

            metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.FLUID_TYPE_COLUMN));
            assertEquals(DataType.text(), metaData.getColumn(DemoTable.FLUID_TYPE_COLUMN).getType());
        }
        assertFalse(session.tableExists(tableName));
    }

    @Test
    public void testTableComparator_NoticeExtraColumn() throws Exception {
        ImmediateCommandExecutor executor = new ImmediateCommandExecutor();

        SessionHelper session = null;
        String tableName = cassandraResource.getUniqueName("tmp_");

        try (TempTable tempTable = CassandraTableProcessor
                .tableBuilder(executor, cassandraResource.getClient(), DemoTable.class).setTableName(tableName)
                .buildTemp()) {

            // Quick sanity check to make sure the table exists and our extra column isn't there.
            session = tempTable.getSession();
            TableMetadata metaData = session.getTableMetadata(tableName);
            assertNull(metaData.getColumn(DemoTable.EXTRA_COLUMN));

            // Let's get in our time machine, and go back to an imaginary timeline where
            // extraColumn existed in our table class.
            session.execute(SchemaBuilder.alterTable(tableName).addColumn(DemoTable.EXTRA_COLUMN)
                    .type(DataType.bigint()));
            metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.EXTRA_COLUMN));
            assertEquals(DataType.bigint(), metaData.getColumn(DemoTable.EXTRA_COLUMN).getType());

            // Now we compare against our schema, and verify that CassandraTableProcessor refuses to fix the discrepancy
            // by default
            CassandraTableProcessor.tableComparator(executor, cassandraResource.getClient(), DemoTable.class)
                    .setTableName(tableName).compare();

            metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.EXTRA_COLUMN));

            // Now check to make sure CTP doesn't drop it, even if the dropColumn string is there, if it's a dry run.
            CassandraTableProcessor.tableComparator(executor, cassandraResource.getClient(), DemoTable.class)
                    .setDropColumns(
                            ImmutableSet.<String> of(tableName + "." + DemoTable.EXTRA_COLUMN.toLowerCase()))
                    .setDryRun(true).setTableName(tableName).compare();

            metaData = session.getTableMetadata(tableName);
            assertNotNull(metaData.getColumn(DemoTable.EXTRA_COLUMN));

            // Now check to make sure CTP drops it, if the dropColumn string is there and it's not a dry run.
            CassandraTableProcessor.tableComparator(executor, cassandraResource.getClient(), DemoTable.class)
                    .setDropColumns(
                            ImmutableSet.<String> of(tableName + "." + DemoTable.EXTRA_COLUMN.toLowerCase()))
                    .setTableName(tableName).compare();

            metaData = session.getTableMetadata(tableName);
            assertNull(metaData.getColumn(DemoTable.EXTRA_COLUMN));
        }
        assertFalse(session.tableExists(tableName));
    }

}
